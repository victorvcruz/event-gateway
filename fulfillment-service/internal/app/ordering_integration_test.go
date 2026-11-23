package app

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"

	domainmigrations "github.com/victorvcruz/event-gateway/fulfillment-service/migrations"
	"github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
	outboxmigrations "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
	"github.com/victorvcruz/event-gateway/shared/testpostgres"
)

// TestFulfillmentRequiresPaymentAndReservationInAnyOrder verifica que
// order.fulfilled só é emitido quando pagamento e reserva estão presentes,
// para as duas ordens de chegada, e que reentregas antes e depois da conclusão
// não emitem o evento outra vez.
func TestFulfillmentRequiresPaymentAndReservationInAnyOrder(t *testing.T) {
	harness := testpostgres.Start(t)
	harness.Apply(t, func(dsn string) error { return outboxmigrations.Apply(dsn) }, domainmigrations.Apply)
	a := New(harness.Pool, zap.NewNop())
	ctx := context.Background()

	countFulfilled := func(t *testing.T, orderID string) int {
		t.Helper()
		var n int
		if err := harness.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE correlation_id=$1 AND event_type='order.fulfilled'`, orderID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	for _, stockFirst := range []bool{true, false} {
		name := "payment then stock"
		if stockFirst {
			name = "stock then payment"
		}
		t.Run(name, func(t *testing.T) {
			orderID, paymentID, reservationID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			paymentEvent := fulfillmentPaymentEvent(t, orderID, paymentID, uuid.New())
			stockEvent := fulfillmentStockEvent(t, orderID, reservationID, uuid.New())
			first, second := a.payment, a.stock
			firstEvent, secondEvent := paymentEvent, stockEvent
			if stockFirst {
				first, second = a.stock, a.payment
				firstEvent, secondEvent = stockEvent, paymentEvent
			}

			if err := first(ctx, firstEvent); err != nil {
				t.Fatalf("first event: %v", err)
			}
			// Reentrega do primeiro evento antes da conclusão: não conclui nem emite.
			if err := first(ctx, firstEvent); err != nil {
				t.Fatalf("redelivery before completion: %v", err)
			}
			if n := countFulfilled(t, orderID); n != 0 {
				t.Fatalf("order.fulfilled emitted with only one prerequisite: %d", n)
			}

			if err := second(ctx, secondEvent); err != nil {
				t.Fatalf("second event: %v", err)
			}
			assertFulfillment(t, harness, orderID, paymentID, reservationID, true, 1)

			for _, deliver := range []func() error{
				func() error { return first(ctx, firstEvent) },
				func() error { return second(ctx, secondEvent) },
			} {
				if err := deliver(); !errors.Is(err, dispatcher.ErrDuplicate) {
					t.Fatalf("redelivery after completion error=%v, want ErrDuplicate", err)
				}
			}
			assertFulfillment(t, harness, orderID, paymentID, reservationID, true, 1)
		})
	}
}
