package app

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
	domainmigrations "github.com/victorvcruz/event-gateway/payment-service/migrations"
	outboxmigrations "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
	"github.com/victorvcruz/event-gateway/shared/testpostgres"
)

// TestPaymentCompensationIsAppliedOnce verifica que a repetição do evento de
// compensação stock.failed — com o mesmo event_id (reentrega) ou com outro
// event_id para a mesma correlação — produz exatamente um estorno, um registro
// de solicitação de estorno (efeito externo modelado por refund_requests) e um
// evento payment.refunded.
func TestPaymentCompensationIsAppliedOnce(t *testing.T) {
	harness := testpostgres.Start(t)
	harness.Apply(t, func(dsn string) error { return outboxmigrations.Apply(dsn) }, domainmigrations.Apply)
	a := New(harness.Pool, 100_000, zap.NewNop())
	ctx := context.Background()

	for _, tc := range []struct {
		name          string
		secondEventID func(first uuid.UUID) uuid.UUID
	}{
		{"same event_id redelivered", func(first uuid.UUID) uuid.UUID { return first }},
		{"different event_id for same correlation", func(uuid.UUID) uuid.UUID { return uuid.New() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orderID := uuid.NewString()
			if err := a.orderCreated(ctx, paymentOrderCreatedEvent(t, orderID, uuid.New())); err != nil {
				t.Fatal(err)
			}
			assertPaymentState(t, harness, orderID, "PROCESSED", 1)

			first := uuid.New()
			if err := a.stockFailed(ctx, stockFailedEvent(t, orderID, first)); err != nil {
				t.Fatalf("first compensation: %v", err)
			}
			if err := a.stockFailed(ctx, stockFailedEvent(t, orderID, tc.secondEventID(first))); !errors.Is(err, dispatcher.ErrDuplicate) {
				t.Fatalf("repeated compensation error=%v, want ErrDuplicate", err)
			}

			assertPaymentState(t, harness, orderID, "REFUNDED", 1)
			assertPaymentOutbox(t, harness, orderID, "payment.refunded", 1)
			var requests int
			if err := harness.Pool.QueryRow(ctx, `SELECT count(*) FROM refund_requests WHERE order_id=$1`, orderID).Scan(&requests); err != nil {
				t.Fatal(err)
			}
			if requests != 1 {
				t.Fatalf("refund requests=%d, want 1", requests)
			}
		})
	}
}

// TestPaymentLateOrderCreatedAfterStockFailedIsNotCharged cobre a ordem
// invertida: stock.failed chega antes de order.created. O pagamento tardio não
// pode ficar cobrado: o serviço registra SKIPPED e não emite payment.processed
// nem payment.refunded.
func TestPaymentLateOrderCreatedAfterStockFailedIsNotCharged(t *testing.T) {
	harness := testpostgres.Start(t)
	harness.Apply(t, func(dsn string) error { return outboxmigrations.Apply(dsn) }, domainmigrations.Apply)
	a := New(harness.Pool, 100_000, zap.NewNop())
	ctx := context.Background()

	orderID := uuid.NewString()
	if err := a.stockFailed(ctx, stockFailedEvent(t, orderID, uuid.New())); err != nil {
		t.Fatalf("early compensation: %v", err)
	}
	if err := a.orderCreated(ctx, paymentOrderCreatedEvent(t, orderID, uuid.New())); err != nil && !errors.Is(err, dispatcher.ErrDuplicate) {
		t.Fatalf("late order.created: %v", err)
	}
	assertPaymentState(t, harness, orderID, "SKIPPED", 1)
	assertPaymentOutbox(t, harness, orderID, "payment.processed", 0)
	assertPaymentOutbox(t, harness, orderID, "payment.refunded", 0)
}
