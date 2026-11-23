package app

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
	"github.com/victorvcruz/event-gateway/gateway/pkg/serviceoutbox"
	"github.com/victorvcruz/event-gateway/shared/contracts"
	"github.com/victorvcruz/event-gateway/shared/failpoint"
	"github.com/victorvcruz/event-gateway/shared/outbox"
	"go.uber.org/zap"
)

type App struct {
	pool   *pgxpool.Pool
	outbox *outbox.Client
	log    *zap.Logger
}

func New(p *pgxpool.Pool, l *zap.Logger) *App {
	return &App{pool: p, outbox: outbox.NewClient(p, ""), log: l}
}
func (a *App) Dispatcher(opts ...dispatcher.Option) *dispatcher.Dispatcher {
	opts = append([]dispatcher.Option{dispatcher.WithLogger(a.log)}, opts...)
	d := dispatcher.New(opts...)
	d.Handle("order.created", a.orderCreated)
	d.Handle("payment.failed", a.paymentFailed)
	return d
}
func (a *App) orderCreated(ctx context.Context, e dispatcher.Event) error {
	var p contracts.OrderCreated
	if contracts.DecodePayload(e.Payload, &p) != nil || contracts.ValidateOrderCreated(p) != nil {
		return dispatcher.ErrBadRequest
	}
	oid, err := uuid.Parse(p.OrderID)
	if err != nil {
		return dispatcher.ErrBadRequest
	}
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, p.OrderID); err != nil {
		return err
	}
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM reservations WHERE order_id=$1)`, oid).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return dispatcher.ErrDuplicate
	}
	rid := uuid.New()
	raw, _ := json.Marshal(p.Items)
	var release bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM release_requests WHERE order_id=$1)`, oid).Scan(&release); err != nil {
		return err
	}
	if release {
		_, err = tx.Exec(ctx, `INSERT INTO reservations(reservation_id,order_id,items,status)VALUES($1,$2,$3,'SKIPPED')`, rid, oid, raw)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	quantities := make(map[string]int, len(p.Items))
	for _, item := range p.Items {
		quantities[item.SKU] += item.Quantity
	}
	items := make([]contracts.Item, 0, len(quantities))
	for sku, quantity := range quantities {
		items = append(items, contracts.Item{SKU: sku, Quantity: quantity})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].SKU < items[j].SKU })
	reason := ""
	unavailable := []string{}
	for _, item := range items {
		var available int
		err = tx.QueryRow(ctx, `SELECT available FROM inventory_items WHERE sku=$1 FOR UPDATE`, item.SKU).Scan(&available)
		if errors.Is(err, pgx.ErrNoRows) {
			reason = "sku_not_found"
			unavailable = append(unavailable, item.SKU)
			continue
		}
		if err != nil {
			return err
		}
		if available < item.Quantity {
			if reason == "" {
				reason = "insufficient_stock"
			}
			unavailable = append(unavailable, item.SKU)
		}
	}
	if reason != "" {
		if _, err = tx.Exec(ctx, `INSERT INTO reservations(reservation_id,order_id,items,status,failure_reason)VALUES($1,$2,$3,'FAILED',$4)`, rid, oid, raw, reason); err != nil {
			return err
		}
		row, x := serviceoutbox.NewRow("stock.failed", e.CorrelationID, contracts.StockFailed{OrderID: p.OrderID, Reason: reason, UnavailableSKUs: unavailable})
		if x != nil {
			return x
		}
		if x = a.outbox.InsertPending(ctx, tx, row); x != nil {
			return x
		}
		return tx.Commit(ctx)
	}
	for _, item := range items {
		if _, err = tx.Exec(ctx, `UPDATE inventory_items SET available=available-$2,updated_at=now()WHERE sku=$1`, item.SKU, item.Quantity); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO reservations(reservation_id,order_id,items,status)VALUES($1,$2,$3,'ACTIVE')`, rid, oid, raw); err != nil {
		return err
	}
	row, err := serviceoutbox.NewRow("stock.reserved", e.CorrelationID, contracts.StockReserved{ReservationID: rid.String(), OrderID: p.OrderID, Items: p.Items})
	if err != nil {
		return err
	}
	if err = a.outbox.InsertPending(ctx, tx, row); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (a *App) paymentFailed(ctx context.Context, e dispatcher.Event) error {
	var p contracts.PaymentFailed
	if contracts.DecodePayload(e.Payload, &p) != nil || contracts.ValidatePaymentFailed(p) != nil {
		return dispatcher.ErrBadRequest
	}
	oid, err := uuid.Parse(p.OrderID)
	if err != nil {
		return dispatcher.ErrBadRequest
	}
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, p.OrderID); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO release_requests(order_id,source_event_id)VALUES($1,$2)ON CONFLICT DO NOTHING`, oid, e.EventID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return dispatcher.ErrDuplicate
	}
	var rid uuid.UUID
	var raw []byte
	var status string
	err = tx.QueryRow(ctx, `SELECT reservation_id,items,status FROM reservations WHERE order_id=$1 FOR UPDATE`, oid).Scan(&rid, &raw, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		failpoint.Hit(ctx, compensationCheckpoint(e))
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}
	if status != "ACTIVE" {
		failpoint.Hit(ctx, compensationCheckpoint(e))
		return tx.Commit(ctx)
	}
	var items []contracts.Item
	if err = json.Unmarshal(raw, &items); err != nil {
		return err
	}
	for _, item := range items {
		if _, err = tx.Exec(ctx, `UPDATE inventory_items SET available=available+$2,updated_at=now()WHERE sku=$1`, item.SKU, item.Quantity); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE reservations SET status='RELEASED',updated_at=now()WHERE order_id=$1`, oid); err != nil {
		return err
	}
	row, err := serviceoutbox.NewRow("stock.released", e.CorrelationID, contracts.StockReleased{ReservationID: rid.String(), OrderID: p.OrderID, ReleasedAt: time.Now().UTC()})
	if err != nil {
		return err
	}
	if err = a.outbox.InsertPending(ctx, tx, row); err != nil {
		return err
	}
	failpoint.Hit(ctx, compensationCheckpoint(e))
	return tx.Commit(ctx)
}

func compensationCheckpoint(e dispatcher.Event) failpoint.Checkpoint {
	return failpoint.Checkpoint{Point: failpoint.InventoryCompensationPreCommit, EventID: e.EventID.String(), EventType: e.EventType, CorrelationID: e.CorrelationID}
}
