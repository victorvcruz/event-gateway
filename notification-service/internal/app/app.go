package app

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
	"github.com/victorvcruz/event-gateway/shared/contracts"
	"go.uber.org/zap"
)

type App struct {
	pool *pgxpool.Pool
	log  *zap.Logger
}

func New(p *pgxpool.Pool, l *zap.Logger) *App { return &App{pool: p, log: l} }
func (a *App) Dispatcher(opts ...dispatcher.Option) *dispatcher.Dispatcher {
	opts = append([]dispatcher.Option{dispatcher.WithLogger(a.log)}, opts...)
	d := dispatcher.New(opts...)
	d.Handle("order.fulfilled", a.handle)
	d.Handle("order.cancelled", a.handle)
	d.Handle("payment.refunded", a.handle)
	d.Handle("stock.released", a.handle)
	return d
}
func (a *App) handle(ctx context.Context, e dispatcher.Event) error {
	orderID, err := extract(e)
	if err != nil {
		return dispatcher.ErrBadRequest
	}
	message := fmt.Sprintf("%s for order %s", e.EventType, orderID)
	tag, err := a.pool.Exec(ctx, `INSERT INTO notifications(event_id,event_type,order_id,correlation_id,message)VALUES($1,$2,$3,$4,$5)ON CONFLICT(event_id)DO NOTHING`, e.EventID, e.EventType, orderID, e.CorrelationID, message)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return dispatcher.ErrDuplicate
	}
	a.log.Info("notification delivered", zap.String("event_type", e.EventType), zap.String("order_id", orderID), zap.String("correlation_id", e.CorrelationID))
	return nil
}
func extract(e dispatcher.Event) (string, error) {
	switch e.EventType {
	case "order.fulfilled":
		var p contracts.OrderFulfilled
		if err := contracts.DecodePayload(e.Payload, &p); err != nil {
			return "", err
		}
		return p.OrderID, contracts.ValidateOrderFulfilled(p)
	case "order.cancelled":
		var p contracts.OrderCancelled
		if err := contracts.DecodePayload(e.Payload, &p); err != nil {
			return "", err
		}
		return p.OrderID, contracts.ValidateOrderCancelled(p)
	case "payment.refunded":
		var p contracts.PaymentRefunded
		if err := contracts.DecodePayload(e.Payload, &p); err != nil {
			return "", err
		}
		return p.OrderID, contracts.ValidatePaymentRefunded(p)
	case "stock.released":
		var p contracts.StockReleased
		if err := contracts.DecodePayload(e.Payload, &p); err != nil {
			return "", err
		}
		return p.OrderID, contracts.ValidateStockReleased(p)
	}
	return "", fmt.Errorf("unsupported")
}
