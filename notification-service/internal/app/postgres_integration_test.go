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
	domainmigrations "github.com/victorvcruz/event-gateway/notification-service/migrations"
	"github.com/victorvcruz/event-gateway/shared/contracts"
	"github.com/victorvcruz/event-gateway/shared/testpostgres"
)

func TestNotificationPostgresInvariants(t *testing.T) {
	harness := testpostgres.Start(t)
	harness.Apply(t, domainmigrations.Apply)
	a := New(harness.Pool, zap.NewNop())

	t.Run("migrations provision an empty database", func(t *testing.T) {
		var exists bool
		if err := harness.Pool.QueryRow(context.Background(),
			`SELECT to_regclass('public.notifications') IS NOT NULL`).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatal("migration did not create notifications")
		}
	})

	t.Run("concurrent duplicate terminal event creates one notification", func(t *testing.T) {
		orderID := uuid.NewString()
		eventID := uuid.New()
		payload, err := json.Marshal(contracts.OrderFulfilled{
			FulfillmentID: uuid.NewString(), OrderID: orderID, DispatchedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatal(err)
		}
		event := dispatcher.Event{
			EventID: eventID, EventType: "order.fulfilled", CorrelationID: orderID,
			Payload: payload, OccurredAt: time.Now().UTC(),
		}
		errs := runNotificationRace(t, func(ctx context.Context) error { return a.handle(ctx, event) })
		var successes, duplicates int
		for _, err := range errs {
			switch {
			case err == nil:
				successes++
			case errors.Is(err, dispatcher.ErrDuplicate):
				duplicates++
			default:
				t.Fatalf("notification handler failed: %v", err)
			}
		}
		if successes != 1 || duplicates != 1 {
			t.Fatalf("successes=%d duplicates=%d", successes, duplicates)
		}

		var count int
		var eventType, storedOrderID, correlationID, channel string
		if err := harness.Pool.QueryRow(context.Background(), `
			SELECT count(*) OVER(),event_type,order_id::text,correlation_id,channel
			FROM notifications WHERE event_id=$1`, eventID).
			Scan(&count, &eventType, &storedOrderID, &correlationID, &channel); err != nil {
			t.Fatal(err)
		}
		if count != 1 || eventType != "order.fulfilled" || storedOrderID != orderID || correlationID != orderID || channel != "log" {
			t.Fatalf("stored notification count=%d type=%s order=%s correlation=%s channel=%s", count, eventType, storedOrderID, correlationID, channel)
		}
	})
}

func runNotificationRace(t *testing.T, run func(context.Context) error) []error {
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
			errs[index] = run(ctx)
		}(i)
	}
	close(start)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("concurrent notification delivery deadlocked")
	}
	return errs
}
