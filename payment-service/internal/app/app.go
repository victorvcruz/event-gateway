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
	"github.com/victorvcruz/event-gateway/shared/failpoint"
	"github.com/victorvcruz/event-gateway/shared/outbox"
	"go.uber.org/zap"
)

type App struct {
	pool   *pgxpool.Pool
	outbox *outbox.Client
	limit  int64
	log    *zap.Logger
}

func New(pool *pgxpool.Pool, limit int64, log *zap.Logger) *App {
	return &App{pool: pool, outbox: outbox.NewClient(pool, ""), limit: limit, log: log}
}
func (a *App) Dispatcher(opts ...dispatcher.Option) *dispatcher.Dispatcher {
	opts = append([]dispatcher.Option{dispatcher.WithLogger(a.log)}, opts...)
	d := dispatcher.New(opts...)
	d.Handle("order.created", a.orderCreated)
	d.Handle("stock.failed", a.stockFailed)
	return d
}
func (a *App) orderCreated(ctx context.Context, e dispatcher.Event) error {
	var p contracts.OrderCreated
	if err := contracts.DecodePayload(e.Payload, &p); err != nil || contracts.ValidateOrderCreated(p) != nil {
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
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM payments WHERE order_id=$1)`, oid).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return dispatcher.ErrDuplicate
	}
	var skip bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM refund_requests WHERE order_id=$1)`, oid).Scan(&skip); err != nil {
		return err
	}
	pid := uuid.New()
	status := "PROCESSED"
	reason := ""
	eventType := "payment.processed"
	var payload any = contracts.PaymentProcessed{PaymentID: pid.String(), OrderID: p.OrderID, AmountCents: p.AmountCents, ChargedAt: timeNow()}
	if skip {
		status = "SKIPPED"
		eventType = ""
	} else if p.AmountCents > a.limit {
		status = "FAILED"
		reason = "approval_limit_exceeded"
		eventType = "payment.failed"
		payload = contracts.PaymentFailed{PaymentID: pid.String(), OrderID: p.OrderID, Reason: reason}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO payments(payment_id,order_id,amount_cents,status,failure_reason)VALUES($1,$2,$3,$4,NULLIF($5,''))`, pid, oid, p.AmountCents, status, reason); err != nil {
		return err
	}
	if eventType != "" {
		row, x := serviceoutbox.NewRow(eventType, e.CorrelationID, payload)
		if x != nil {
			return x
		}
		if x = a.outbox.InsertPending(ctx, tx, row); x != nil {
			return x
		}
	}
	return tx.Commit(ctx)
}
func (a *App) stockFailed(ctx context.Context, e dispatcher.Event) error {
	var p contracts.StockFailed
	if contracts.DecodePayload(e.Payload, &p) != nil || contracts.ValidateStockFailed(p) != nil {
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
	tag, err := tx.Exec(ctx, `INSERT INTO refund_requests(order_id,source_event_id)VALUES($1,$2)ON CONFLICT DO NOTHING`, oid, e.EventID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return dispatcher.ErrDuplicate
	}
	var pid uuid.UUID
	var amount int64
	var status string
	err = tx.QueryRow(ctx, `SELECT payment_id,amount_cents,status FROM payments WHERE order_id=$1 FOR UPDATE`, oid).Scan(&pid, &amount, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		failpoint.Hit(ctx, compensationCheckpoint(e))
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}
	if status != "PROCESSED" {
		failpoint.Hit(ctx, compensationCheckpoint(e))
		return tx.Commit(ctx)
	}
	if _, err = tx.Exec(ctx, `UPDATE payments SET status='REFUNDED',updated_at=now()WHERE order_id=$1`, oid); err != nil {
		return err
	}
	row, err := serviceoutbox.NewRow("payment.refunded", e.CorrelationID, contracts.PaymentRefunded{PaymentID: pid.String(), OrderID: p.OrderID, AmountCents: amount, RefundedAt: timeNow()})
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
	return failpoint.Checkpoint{Point: failpoint.PaymentCompensationPreCommit, EventID: e.EventID.String(), EventType: e.EventType, CorrelationID: e.CorrelationID}
}

var nowFunc = time.Now

func timeNow() time.Time { return nowFunc().UTC() }
