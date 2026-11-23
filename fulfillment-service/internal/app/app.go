package app

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
	"github.com/victorvcruz/event-gateway/gateway/pkg/serviceoutbox"
	"github.com/victorvcruz/event-gateway/shared/contracts"
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
	d.Handle("payment.processed", a.payment)
	d.Handle("stock.reserved", a.stock)
	return d
}
func (a *App) payment(ctx context.Context, e dispatcher.Event) error {
	var p contracts.PaymentProcessed
	if contracts.DecodePayload(e.Payload, &p) != nil || contracts.ValidatePaymentProcessed(p) != nil {
		return dispatcher.ErrBadRequest
	}
	return a.record(ctx, e.CorrelationID, p.OrderID, p.PaymentID, "")
}
func (a *App) stock(ctx context.Context, e dispatcher.Event) error {
	var p contracts.StockReserved
	if contracts.DecodePayload(e.Payload, &p) != nil || contracts.ValidateStockReserved(p) != nil {
		return dispatcher.ErrBadRequest
	}
	return a.record(ctx, e.CorrelationID, p.OrderID, "", p.ReservationID)
}
func (a *App) record(ctx context.Context, corr, orderID, paymentID, reservationID string) error {
	oid, err := uuid.Parse(orderID)
	if err != nil {
		return dispatcher.ErrBadRequest
	}
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `INSERT INTO fulfillment_state(order_id,payment_id,reservation_id)VALUES($1,NULLIF($2,'')::uuid,NULLIF($3,'')::uuid)ON CONFLICT(order_id)DO UPDATE SET payment_id=COALESCE(fulfillment_state.payment_id,EXCLUDED.payment_id),reservation_id=COALESCE(fulfillment_state.reservation_id,EXCLUDED.reservation_id),updated_at=now()`, oid, paymentID, reservationID)
	if err != nil {
		return err
	}
	var pid, rid *uuid.UUID
	var fid *uuid.UUID
	err = tx.QueryRow(ctx, `SELECT payment_id,reservation_id,fulfillment_id FROM fulfillment_state WHERE order_id=$1 FOR UPDATE`, oid).Scan(&pid, &rid, &fid)
	if err != nil {
		return err
	}
	if fid != nil {
		return dispatcher.ErrDuplicate
	}
	if pid == nil || rid == nil {
		return tx.Commit(ctx)
	}
	newID := uuid.New()
	now := time.Now().UTC()
	tag, err := tx.Exec(ctx, `UPDATE fulfillment_state SET fulfillment_id=$2,fulfilled_at=$3,updated_at=$3 WHERE order_id=$1 AND fulfillment_id IS NULL`, oid, newID, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return dispatcher.ErrDuplicate
	}
	row, err := serviceoutbox.NewRow("order.fulfilled", corr, contracts.OrderFulfilled{FulfillmentID: newID.String(), OrderID: orderID, DispatchedAt: now})
	if err != nil {
		return err
	}
	if err = a.outbox.InsertPending(ctx, tx, row); err != nil && !errors.Is(err, outbox.ErrDuplicate) {
		return err
	}
	return tx.Commit(ctx)
}

var _ = pgx.ErrNoRows
