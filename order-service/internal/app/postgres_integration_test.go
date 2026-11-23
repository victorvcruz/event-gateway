package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	domainmigrations "github.com/victorvcruz/event-gateway/order-service/migrations"
	"github.com/victorvcruz/event-gateway/shared/contracts"
	outboxmigrations "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
	"github.com/victorvcruz/event-gateway/shared/testpostgres"
)

func TestOrderPostgresInvariants(t *testing.T) {
	harness := testpostgres.Start(t)
	harness.Apply(t, func(dsn string) error { return outboxmigrations.Apply(dsn) }, domainmigrations.Apply)
	a := New(harness.Pool, zap.NewNop(), nil)

	t.Run("migrations provision an empty database", func(t *testing.T) {
		for _, table := range []string{"orders", "outbox"} {
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

	t.Run("concurrent idempotency key creates one order and outbox event", func(t *testing.T) {
		key := "integration-" + uuid.NewString()
		req := CreateRequest{
			CustomerID: "integration", AmountCents: 100,
			Items: []contracts.Item{{SKU: "SKU-001", Quantity: 1}},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		start := make(chan struct{})
		orders := make([]Order, 2)
		created := make([]bool, 2)
		errs := make([]error, 2)
		var wg sync.WaitGroup
		for i := range orders {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				<-start
				orders[index], created[index], errs[index] = a.create(ctx, key, req)
			}(i)
		}
		close(start)
		waitOrderRace(t, ctx, &wg)
		for _, err := range errs {
			if err != nil {
				t.Fatalf("create failed: %v", err)
			}
		}
		if created[0] == created[1] {
			t.Fatalf("created flags=%v, want exactly one true", created)
		}
		if orders[0].OrderID != orders[1].OrderID {
			t.Fatalf("different orders for same key: %s %s", orders[0].OrderID, orders[1].OrderID)
		}
		assertOrderCounts(t, harness, key, orders[0].CorrelationID, 1, 1)

		repeated, repeatedCreated, err := a.create(context.Background(), key, req)
		if err != nil || repeatedCreated || repeated.OrderID != orders[0].OrderID {
			t.Fatalf("idempotent repeat=(%+v,%v,%v)", repeated, repeatedCreated, err)
		}
		assertOrderCounts(t, harness, key, orders[0].CorrelationID, 1, 1)
	})

	t.Run("failed outbox insert rolls order back", func(t *testing.T) {
		key := "integration-" + uuid.NewString()
		var ordersBefore, eventsBefore int
		if err := harness.Pool.QueryRow(context.Background(), `SELECT count(*) FROM orders`).Scan(&ordersBefore); err != nil {
			t.Fatal(err)
		}
		if err := harness.Pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox`).Scan(&eventsBefore); err != nil {
			t.Fatal(err)
		}
		if _, err := harness.Pool.Exec(context.Background(), `
			CREATE OR REPLACE FUNCTION reject_order_test_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN RAISE EXCEPTION 'forced outbox failure'; END $$;
			CREATE TRIGGER reject_order_test_outbox BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION reject_order_test_outbox()`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = harness.Pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS reject_order_test_outbox ON outbox; DROP FUNCTION IF EXISTS reject_order_test_outbox()`)
		})

		_, _, err := a.create(context.Background(), key, CreateRequest{
			CustomerID: "integration", AmountCents: 100,
			Items: []contracts.Item{{SKU: "SKU-001", Quantity: 1}},
		})
		if err == nil {
			t.Fatal("expected forced outbox failure")
		}
		var ordersAfter, eventsAfter int
		if err := harness.Pool.QueryRow(context.Background(), `SELECT count(*) FROM orders`).Scan(&ordersAfter); err != nil {
			t.Fatal(err)
		}
		if err := harness.Pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox`).Scan(&eventsAfter); err != nil {
			t.Fatal(err)
		}
		if ordersAfter != ordersBefore || eventsAfter != eventsBefore {
			t.Fatalf("failed transaction changed totals: orders %d->%d events %d->%d", ordersBefore, ordersAfter, eventsBefore, eventsAfter)
		}

		if _, err := harness.Pool.Exec(context.Background(), `DROP TRIGGER reject_order_test_outbox ON outbox; DROP FUNCTION reject_order_test_outbox()`); err != nil {
			t.Fatal(err)
		}
	})
}

func waitOrderRace(t *testing.T, ctx context.Context, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("concurrent order creation deadlocked")
	}
}

func assertOrderCounts(t *testing.T, harness *testpostgres.Harness, key, correlationID string, wantOrders, wantEvents int) {
	t.Helper()
	var orders, events int
	if err := harness.Pool.QueryRow(context.Background(), `SELECT count(*) FROM orders WHERE idempotency_key=$1`, key).Scan(&orders); err != nil {
		t.Fatal(err)
	}
	if err := harness.Pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE correlation_id=$1 AND event_type='order.created'`, correlationID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if orders != wantOrders || events != wantEvents {
		t.Fatalf("orders=%d events=%d, want %d/%d", orders, events, wantOrders, wantEvents)
	}
}
