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

	domainmigrations "github.com/victorvcruz/event-gateway/fulfillment-service/migrations"
	"github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
	"github.com/victorvcruz/event-gateway/shared/contracts"
	outboxmigrations "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
	"github.com/victorvcruz/event-gateway/shared/testpostgres"
)

func TestFulfillmentPostgresInvariants(t *testing.T) {
	harness := testpostgres.Start(t)
	harness.Apply(t, func(dsn string) error { return outboxmigrations.Apply(dsn) }, domainmigrations.Apply)
	a := New(harness.Pool, zap.NewNop())

	t.Run("migrations provision an empty database", func(t *testing.T) {
		for _, table := range []string{"fulfillment_state", "outbox"} {
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

	t.Run("payment and stock delivered concurrently create one fulfillment", func(t *testing.T) {
		orderID := uuid.NewString()
		paymentID := uuid.NewString()
		reservationID := uuid.NewString()
		paymentEvent := fulfillmentPaymentEvent(t, orderID, paymentID, uuid.New())
		stockEvent := fulfillmentStockEvent(t, orderID, reservationID, uuid.New())
		errs := runFulfillmentRace(t, func(index int, ctx context.Context) error {
			if index == 0 {
				return a.payment(ctx, paymentEvent)
			}
			return a.stock(ctx, stockEvent)
		})
		for _, err := range errs {
			if err != nil {
				t.Fatalf("aggregation handler failed: %v", err)
			}
		}
		assertFulfillment(t, harness, orderID, paymentID, reservationID, true, 1)

		for _, deliver := range []func() error{
			func() error { return a.stock(context.Background(), stockEvent) },
			func() error { return a.payment(context.Background(), paymentEvent) },
		} {
			if err := deliver(); !errors.Is(err, dispatcher.ErrDuplicate) {
				t.Fatalf("terminal redelivery error=%v, want ErrDuplicate", err)
			}
		}
		assertFulfillment(t, harness, orderID, paymentID, reservationID, true, 1)
	})

	t.Run("outbox failure rolls back completing event", func(t *testing.T) {
		orderID := uuid.NewString()
		paymentID := uuid.NewString()
		reservationID := uuid.NewString()
		if err := a.payment(context.Background(), fulfillmentPaymentEvent(t, orderID, paymentID, uuid.New())); err != nil {
			t.Fatal(err)
		}
		if _, err := harness.Pool.Exec(context.Background(), `
			CREATE OR REPLACE FUNCTION reject_fulfillment_test_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN IF NEW.correlation_id = '`+orderID+`' THEN RAISE EXCEPTION 'forced outbox failure'; END IF; RETURN NEW; END $$;
			CREATE TRIGGER reject_fulfillment_test_outbox BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION reject_fulfillment_test_outbox()`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = harness.Pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS reject_fulfillment_test_outbox ON outbox; DROP FUNCTION IF EXISTS reject_fulfillment_test_outbox()`)
		})

		if err := a.stock(context.Background(), fulfillmentStockEvent(t, orderID, reservationID, uuid.New())); err == nil {
			t.Fatal("expected forced outbox failure")
		}
		assertFulfillment(t, harness, orderID, paymentID, "", false, 0)

		if _, err := harness.Pool.Exec(context.Background(), `DROP TRIGGER reject_fulfillment_test_outbox ON outbox; DROP FUNCTION reject_fulfillment_test_outbox()`); err != nil {
			t.Fatal(err)
		}
	})
}

func fulfillmentPaymentEvent(t *testing.T, orderID, paymentID string, eventID uuid.UUID) dispatcher.Event {
	t.Helper()
	payload, err := json.Marshal(contracts.PaymentProcessed{
		PaymentID: paymentID, OrderID: orderID, AmountCents: 100, ChargedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher.Event{EventID: eventID, EventType: "payment.processed", CorrelationID: orderID, Payload: payload, OccurredAt: time.Now().UTC()}
}

func fulfillmentStockEvent(t *testing.T, orderID, reservationID string, eventID uuid.UUID) dispatcher.Event {
	t.Helper()
	payload, err := json.Marshal(contracts.StockReserved{
		ReservationID: reservationID, OrderID: orderID,
		Items: []contracts.Item{{SKU: "SKU-001", Quantity: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher.Event{EventID: eventID, EventType: "stock.reserved", CorrelationID: orderID, Payload: payload, OccurredAt: time.Now().UTC()}
}

func runFulfillmentRace(t *testing.T, run func(int, context.Context) error) []error {
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
		t.Fatal("fulfillment aggregation deadlocked")
	}
	return errs
}

func assertFulfillment(t *testing.T, harness *testpostgres.Harness, orderID, paymentID, reservationID string, fulfilled bool, wantEvents int) {
	t.Helper()
	var gotPayment, gotReservation *uuid.UUID
	var fulfillmentID *uuid.UUID
	if err := harness.Pool.QueryRow(context.Background(), `SELECT payment_id,reservation_id,fulfillment_id FROM fulfillment_state WHERE order_id=$1`, orderID).Scan(&gotPayment, &gotReservation, &fulfillmentID); err != nil {
		t.Fatal(err)
	}
	if gotPayment == nil || gotPayment.String() != paymentID {
		t.Fatalf("payment_id=%v, want %s", gotPayment, paymentID)
	}
	if reservationID == "" {
		if gotReservation != nil {
			t.Fatalf("reservation_id=%v, want nil", gotReservation)
		}
	} else if gotReservation == nil || gotReservation.String() != reservationID {
		t.Fatalf("reservation_id=%v, want %s", gotReservation, reservationID)
	}
	if (fulfillmentID != nil) != fulfilled {
		t.Fatalf("fulfillment_id=%v, fulfilled=%v", fulfillmentID, fulfilled)
	}
	var events int
	if err := harness.Pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE correlation_id=$1 AND event_type='order.fulfilled'`, orderID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != wantEvents {
		t.Fatalf("fulfillment events=%d, want %d", events, wantEvents)
	}
}
