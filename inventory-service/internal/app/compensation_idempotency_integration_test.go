package app

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
	domainmigrations "github.com/victorvcruz/event-gateway/inventory-service/migrations"
	outboxmigrations "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
	"github.com/victorvcruz/event-gateway/shared/testpostgres"
)

// TestInventoryCompensationIsAppliedOnce verifica que a repetição de
// payment.failed — com o mesmo event_id ou com outro event_id para a mesma
// correlação — devolve o estoque uma única vez, grava um único release_request
// e emite um único stock.released.
func TestInventoryCompensationIsAppliedOnce(t *testing.T) {
	harness := testpostgres.Start(t)
	harness.Apply(t, func(dsn string) error { return outboxmigrations.Apply(dsn) }, domainmigrations.Apply)
	a := New(harness.Pool, zap.NewNop())
	ctx := context.Background()

	for _, tc := range []struct {
		name          string
		secondEventID func(first uuid.UUID) uuid.UUID
	}{
		{"same event_id redelivered", func(first uuid.UUID) uuid.UUID { return first }},
		{"different event_id for same correlation", func(uuid.UUID) uuid.UUID { return uuid.New() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sku := insertInventorySKU(t, harness, 5)
			orderID := uuid.NewString()
			if err := a.orderCreated(ctx, inventoryOrderCreatedEvent(t, orderID, sku, 2, uuid.New())); err != nil {
				t.Fatal(err)
			}
			assertReservationState(t, harness, orderID, "ACTIVE", 1)
			assertInventoryBalance(t, harness, sku, 3)

			first := uuid.New()
			if err := a.paymentFailed(ctx, paymentFailedEvent(t, orderID, first)); err != nil {
				t.Fatalf("first compensation: %v", err)
			}
			if err := a.paymentFailed(ctx, paymentFailedEvent(t, orderID, tc.secondEventID(first))); !errors.Is(err, dispatcher.ErrDuplicate) {
				t.Fatalf("repeated compensation error=%v, want ErrDuplicate", err)
			}

			assertReservationState(t, harness, orderID, "RELEASED", 1)
			assertInventoryBalance(t, harness, sku, 5)
			assertInventoryOutbox(t, harness, orderID, "stock.released", 1)
			var requests int
			if err := harness.Pool.QueryRow(ctx, `SELECT count(*) FROM release_requests WHERE order_id=$1`, orderID).Scan(&requests); err != nil {
				t.Fatal(err)
			}
			if requests != 1 {
				t.Fatalf("release requests=%d, want 1", requests)
			}
		})
	}
}

// TestInventoryLateOrderCreatedAfterPaymentFailedReservesNothing cobre a ordem
// invertida: payment.failed chega antes de order.created. A reserva tardia não
// pode prender estoque: o serviço registra SKIPPED, mantém o saldo e não emite
// stock.reserved nem stock.released.
func TestInventoryLateOrderCreatedAfterPaymentFailedReservesNothing(t *testing.T) {
	harness := testpostgres.Start(t)
	harness.Apply(t, func(dsn string) error { return outboxmigrations.Apply(dsn) }, domainmigrations.Apply)
	a := New(harness.Pool, zap.NewNop())
	ctx := context.Background()

	sku := insertInventorySKU(t, harness, 5)
	orderID := uuid.NewString()
	if err := a.paymentFailed(ctx, paymentFailedEvent(t, orderID, uuid.New())); err != nil {
		t.Fatalf("early compensation: %v", err)
	}
	if err := a.orderCreated(ctx, inventoryOrderCreatedEvent(t, orderID, sku, 2, uuid.New())); err != nil && !errors.Is(err, dispatcher.ErrDuplicate) {
		t.Fatalf("late order.created: %v", err)
	}
	assertReservationState(t, harness, orderID, "SKIPPED", 1)
	assertInventoryBalance(t, harness, sku, 5)
	assertInventoryOutbox(t, harness, orderID, "stock.reserved", 0)
	assertInventoryOutbox(t, harness, orderID, "stock.released", 0)
}
