package app

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
	domainmigrations "github.com/victorvcruz/event-gateway/inventory-service/migrations"
	"github.com/victorvcruz/event-gateway/shared/contracts"
	outboxmigrations "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
	"github.com/victorvcruz/event-gateway/shared/testpostgres"
)

func TestInventoryPostgresInvariants(t *testing.T) {
	harness := testpostgres.Start(t)
	harness.Apply(t, func(dsn string) error { return outboxmigrations.Apply(dsn) }, domainmigrations.Apply)
	a := New(harness.Pool, zap.NewNop())

	t.Run("migrations provision an empty database", func(t *testing.T) {
		for _, table := range []string{"inventory_items", "reservations", "release_requests", "outbox"} {
			var exists bool
			if err := harness.Pool.QueryRow(context.Background(),
				`SELECT to_regclass('public.' || $1) IS NOT NULL`, table).Scan(&exists); err != nil {
				t.Fatal(err)
			}
			if !exists {
				t.Fatalf("migration did not create %s", table)
			}
		}
	})

	t.Run("duplicate concurrent order created reserves once", func(t *testing.T) {
		sku := insertInventorySKU(t, harness, 5)
		orderID := uuid.NewString()
		events := []dispatcher.Event{
			inventoryOrderCreatedEvent(t, orderID, sku, 2, uuid.New()),
			inventoryOrderCreatedEvent(t, orderID, sku, 2, uuid.New()),
		}
		errs := runInventoryRace(t, func(index int, ctx context.Context) error {
			return a.orderCreated(ctx, events[index])
		})
		assertInventoryOneSuccessOneDuplicate(t, errs)
		if err := a.orderCreated(context.Background(), events[0]); !errors.Is(err, dispatcher.ErrDuplicate) {
			t.Fatalf("redelivery error=%v, want ErrDuplicate", err)
		}

		assertReservationState(t, harness, orderID, "ACTIVE", 1)
		assertInventoryOutbox(t, harness, orderID, "stock.reserved", 1)
		assertInventoryBalance(t, harness, sku, 3)
	})

	t.Run("two orders competing for one unit never make stock negative", func(t *testing.T) {
		sku := insertInventorySKU(t, harness, 1)
		orders := []string{uuid.NewString(), uuid.NewString()}
		events := []dispatcher.Event{
			inventoryOrderCreatedEvent(t, orders[0], sku, 1, uuid.New()),
			inventoryOrderCreatedEvent(t, orders[1], sku, 1, uuid.New()),
		}
		errs := runInventoryRace(t, func(index int, ctx context.Context) error {
			return a.orderCreated(ctx, events[index])
		})
		for _, err := range errs {
			if err != nil {
				t.Fatalf("competing order failed unexpectedly: %v", err)
			}
		}

		var active, failed int
		if err := harness.Pool.QueryRow(context.Background(), `
			SELECT count(*) FILTER (WHERE status='ACTIVE'), count(*) FILTER (WHERE status='FAILED')
			FROM reservations WHERE order_id IN ($1,$2)`, orders[0], orders[1]).Scan(&active, &failed); err != nil {
			t.Fatal(err)
		}
		if active != 1 || failed != 1 {
			t.Fatalf("active=%d failed=%d, want one each", active, failed)
		}
		assertInventoryBalance(t, harness, sku, 0)
		var reservedEvents, failedEvents int
		if err := harness.Pool.QueryRow(context.Background(), `
			SELECT count(*) FILTER (WHERE event_type='stock.reserved'), count(*) FILTER (WHERE event_type='stock.failed')
			FROM outbox WHERE correlation_id IN ($1,$2)`, orders[0], orders[1]).Scan(&reservedEvents, &failedEvents); err != nil {
			t.Fatal(err)
		}
		if reservedEvents != 1 || failedEvents != 1 {
			t.Fatalf("reserved events=%d failed events=%d", reservedEvents, failedEvents)
		}
	})

	t.Run("failed outbox insert rolls reservation and balance back", func(t *testing.T) {
		sku := insertInventorySKU(t, harness, 3)
		orderID := uuid.NewString()
		if _, err := harness.Pool.Exec(context.Background(), `
			CREATE OR REPLACE FUNCTION reject_inventory_test_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN IF NEW.correlation_id = '`+orderID+`' THEN RAISE EXCEPTION 'forced outbox failure'; END IF; RETURN NEW; END $$;
			CREATE TRIGGER reject_inventory_test_outbox BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION reject_inventory_test_outbox()`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = harness.Pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS reject_inventory_test_outbox ON outbox; DROP FUNCTION IF EXISTS reject_inventory_test_outbox()`)
		})

		if err := a.orderCreated(context.Background(), inventoryOrderCreatedEvent(t, orderID, sku, 2, uuid.New())); err == nil {
			t.Fatal("expected forced outbox failure")
		}
		assertReservationState(t, harness, orderID, "", 0)
		assertInventoryOutbox(t, harness, orderID, "stock.reserved", 0)
		assertInventoryBalance(t, harness, sku, 3)

		if _, err := harness.Pool.Exec(context.Background(), `DROP TRIGGER reject_inventory_test_outbox ON outbox; DROP FUNCTION reject_inventory_test_outbox()`); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("order created racing payment failed restores all stock", func(t *testing.T) {
		sku := insertInventorySKU(t, harness, 4)
		orderID := uuid.NewString()
		created := inventoryOrderCreatedEvent(t, orderID, sku, 3, uuid.New())
		failed := paymentFailedEvent(t, orderID, uuid.New())
		errs := runInventoryRace(t, func(index int, ctx context.Context) error {
			if index == 0 {
				return a.orderCreated(ctx, created)
			}
			return a.paymentFailed(ctx, failed)
		})
		for _, err := range errs {
			if err != nil {
				t.Fatalf("race handler failed: %v", err)
			}
		}

		var status string
		if err := harness.Pool.QueryRow(context.Background(), `SELECT status FROM reservations WHERE order_id=$1`, orderID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "SKIPPED" && status != "RELEASED" {
			t.Fatalf("unreconciled reservation status: %s", status)
		}
		assertInventoryBalance(t, harness, sku, 4)
		var requests int
		if err := harness.Pool.QueryRow(context.Background(), `SELECT count(*) FROM release_requests WHERE order_id=$1`, orderID).Scan(&requests); err != nil {
			t.Fatal(err)
		}
		if requests != 1 {
			t.Fatalf("release requests=%d, want 1", requests)
		}
		var reserved, released int
		if err := harness.Pool.QueryRow(context.Background(), `SELECT count(*) FILTER (WHERE event_type='stock.reserved'), count(*) FILTER (WHERE event_type='stock.released') FROM outbox WHERE correlation_id=$1`, orderID).Scan(&reserved, &released); err != nil {
			t.Fatal(err)
		}
		if status == "SKIPPED" && (reserved != 0 || released != 0) {
			t.Fatalf("skipped reservation emitted events: reserved=%d released=%d", reserved, released)
		}
		if status == "RELEASED" && (reserved != 1 || released != 1) {
			t.Fatalf("released reservation events: reserved=%d released=%d", reserved, released)
		}
	})
}

func insertInventorySKU(t *testing.T, harness *testpostgres.Harness, available int) string {
	t.Helper()
	sku := "TEST-" + uuid.NewString()
	if _, err := harness.Pool.Exec(context.Background(), `INSERT INTO inventory_items(sku,available) VALUES($1,$2)`, sku, available); err != nil {
		t.Fatal(err)
	}
	return sku
}

func inventoryOrderCreatedEvent(t *testing.T, orderID, sku string, quantity int, eventID uuid.UUID) dispatcher.Event {
	t.Helper()
	payload, err := json.Marshal(contracts.OrderCreated{
		OrderID: orderID, CustomerID: "integration", AmountCents: 100,
		Items: []contracts.Item{{SKU: sku, Quantity: quantity}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher.Event{EventID: eventID, EventType: "order.created", CorrelationID: orderID, Payload: payload, OccurredAt: time.Now().UTC()}
}

func paymentFailedEvent(t *testing.T, orderID string, eventID uuid.UUID) dispatcher.Event {
	t.Helper()
	payload, err := json.Marshal(contracts.PaymentFailed{PaymentID: uuid.NewString(), OrderID: orderID, Reason: "declined"})
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher.Event{EventID: eventID, EventType: "payment.failed", CorrelationID: orderID, Payload: payload, OccurredAt: time.Now().UTC()}
}

func runInventoryRace(t *testing.T, run func(int, context.Context) error) []error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			errs[index] = run(index, ctx)
		}(i)
	}
	close(start)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("concurrent handlers deadlocked")
	}
	return errs
}

func assertInventoryOneSuccessOneDuplicate(t *testing.T, errs []error) {
	t.Helper()
	var successes, duplicates int
	for _, err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, dispatcher.ErrDuplicate):
			duplicates++
		default:
			t.Fatalf("unexpected handler error: %v", err)
		}
	}
	if successes != 1 || duplicates != 1 {
		t.Fatalf("successes=%d duplicates=%d", successes, duplicates)
	}
}

func assertReservationState(t *testing.T, harness *testpostgres.Harness, orderID, status string, want int) {
	t.Helper()
	var count int
	if err := harness.Pool.QueryRow(context.Background(), `SELECT count(*) FROM reservations WHERE order_id=$1 AND ($2='' OR status=$2)`, orderID, status).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("reservations=%d, want %d", count, want)
	}
}

func assertInventoryOutbox(t *testing.T, harness *testpostgres.Harness, correlationID, eventType string, want int) {
	t.Helper()
	var count int
	if err := harness.Pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE correlation_id=$1 AND event_type=$2`, correlationID, eventType).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("outbox rows=%d, want %d", count, want)
	}
}

func assertInventoryBalance(t *testing.T, harness *testpostgres.Harness, sku string, want int) {
	t.Helper()
	var available int
	if err := harness.Pool.QueryRow(context.Background(), `SELECT available FROM inventory_items WHERE sku=$1`, sku).Scan(&available); err != nil {
		t.Fatal(err)
	}
	if available != want {
		t.Fatalf("available=%d, want %d", available, want)
	}
}
