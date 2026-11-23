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
	domainmigrations "github.com/victorvcruz/event-gateway/payment-service/migrations"
	"github.com/victorvcruz/event-gateway/shared/contracts"
	outboxmigrations "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
	"github.com/victorvcruz/event-gateway/shared/testpostgres"
)

func TestPaymentPostgresInvariants(t *testing.T) {
	harness := testpostgres.Start(t)
	harness.Apply(t, func(dsn string) error { return outboxmigrations.Apply(dsn) }, domainmigrations.Apply)
	a := New(harness.Pool, 100_000, zap.NewNop())

	t.Run("migrations provision an empty database", func(t *testing.T) {
		for _, table := range []string{"payments", "refund_requests", "outbox"} {
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

	t.Run("duplicate concurrent order created commits one payment and event", func(t *testing.T) {
		orderID := uuid.NewString()
		events := []dispatcher.Event{
			paymentOrderCreatedEvent(t, orderID, uuid.New()),
			paymentOrderCreatedEvent(t, orderID, uuid.New()),
		}
		errs := runPaymentRace(t, func(index int, ctx context.Context) error {
			return a.orderCreated(ctx, events[index])
		})
		assertOneSuccessOneDuplicate(t, errs)
		if err := a.orderCreated(context.Background(), events[0]); !errors.Is(err, dispatcher.ErrDuplicate) {
			t.Fatalf("redelivery error=%v, want ErrDuplicate", err)
		}

		assertPaymentState(t, harness, orderID, "PROCESSED", 1)
		assertPaymentOutbox(t, harness, orderID, "payment.processed", 1)
	})

	t.Run("failed outbox insert rolls payment back", func(t *testing.T) {
		orderID := uuid.NewString()
		if _, err := harness.Pool.Exec(context.Background(), `
			CREATE OR REPLACE FUNCTION reject_payment_test_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN IF NEW.correlation_id = '`+orderID+`' THEN RAISE EXCEPTION 'forced outbox failure'; END IF; RETURN NEW; END $$;
			CREATE TRIGGER reject_payment_test_outbox BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION reject_payment_test_outbox()`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = harness.Pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS reject_payment_test_outbox ON outbox; DROP FUNCTION IF EXISTS reject_payment_test_outbox()`)
		})

		if err := a.orderCreated(context.Background(), paymentOrderCreatedEvent(t, orderID, uuid.New())); err == nil {
			t.Fatal("expected forced outbox failure")
		}
		assertPaymentState(t, harness, orderID, "", 0)
		assertPaymentOutbox(t, harness, orderID, "payment.processed", 0)

		if _, err := harness.Pool.Exec(context.Background(), `DROP TRIGGER reject_payment_test_outbox ON outbox; DROP FUNCTION reject_payment_test_outbox()`); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("order created racing stock failed is fully reconciled", func(t *testing.T) {
		orderID := uuid.NewString()
		created := paymentOrderCreatedEvent(t, orderID, uuid.New())
		failed := stockFailedEvent(t, orderID, uuid.New())
		errs := runPaymentRace(t, func(index int, ctx context.Context) error {
			if index == 0 {
				return a.orderCreated(ctx, created)
			}
			return a.stockFailed(ctx, failed)
		})
		for _, err := range errs {
			if err != nil {
				t.Fatalf("race handler failed: %v", err)
			}
		}

		var status string
		if err := harness.Pool.QueryRow(context.Background(), `SELECT status FROM payments WHERE order_id=$1`, orderID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "SKIPPED" && status != "REFUNDED" {
			t.Fatalf("unreconciled payment status: %s", status)
		}
		var requests int
		if err := harness.Pool.QueryRow(context.Background(), `SELECT count(*) FROM refund_requests WHERE order_id=$1`, orderID).Scan(&requests); err != nil {
			t.Fatal(err)
		}
		if requests != 1 {
			t.Fatalf("refund requests=%d, want 1", requests)
		}
		var processed, refunded int
		if err := harness.Pool.QueryRow(context.Background(), `SELECT count(*) FILTER (WHERE event_type='payment.processed'), count(*) FILTER (WHERE event_type='payment.refunded') FROM outbox WHERE correlation_id=$1`, orderID).Scan(&processed, &refunded); err != nil {
			t.Fatal(err)
		}
		if status == "SKIPPED" && (processed != 0 || refunded != 0) {
			t.Fatalf("skipped payment emitted events: processed=%d refunded=%d", processed, refunded)
		}
		if status == "REFUNDED" && (processed != 1 || refunded != 1) {
			t.Fatalf("refunded payment events: processed=%d refunded=%d", processed, refunded)
		}
	})
}

func paymentOrderCreatedEvent(t *testing.T, orderID string, eventID uuid.UUID) dispatcher.Event {
	t.Helper()
	payload, err := json.Marshal(contracts.OrderCreated{
		OrderID: orderID, CustomerID: "integration", AmountCents: 100,
		Items: []contracts.Item{{SKU: "SKU-001", Quantity: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher.Event{EventID: eventID, EventType: "order.created", CorrelationID: orderID, Payload: payload, OccurredAt: time.Now().UTC()}
}

func stockFailedEvent(t *testing.T, orderID string, eventID uuid.UUID) dispatcher.Event {
	t.Helper()
	payload, err := json.Marshal(contracts.StockFailed{OrderID: orderID, Reason: "insufficient_stock", UnavailableSKUs: []string{"SKU-X"}})
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher.Event{EventID: eventID, EventType: "stock.failed", CorrelationID: orderID, Payload: payload, OccurredAt: time.Now().UTC()}
}

func runPaymentRace(t *testing.T, run func(int, context.Context) error) []error {
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

func assertOneSuccessOneDuplicate(t *testing.T, errs []error) {
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

func assertPaymentState(t *testing.T, harness *testpostgres.Harness, orderID, status string, want int) {
	t.Helper()
	var count int
	if err := harness.Pool.QueryRow(context.Background(), `SELECT count(*) FROM payments WHERE order_id=$1 AND ($2='' OR status=$2)`, orderID, status).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("payments=%d, want %d", count, want)
	}
}

func assertPaymentOutbox(t *testing.T, harness *testpostgres.Harness, correlationID, eventType string, want int) {
	t.Helper()
	var count int
	if err := harness.Pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE correlation_id=$1 AND event_type=$2`, correlationID, eventType).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("outbox rows=%d, want %d", count, want)
	}
}
