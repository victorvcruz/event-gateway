package schema_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
	"github.com/victorvcruz/event-gateway/gateway/pkg/client"
	"github.com/victorvcruz/event-gateway/shared/contracts"
)

func TestBusinessPayloadTypesMatchEmbeddedSchemas(t *testing.T) {
	now := time.Now().UTC()
	id := uuid.NewString()
	order := uuid.NewString()
	payment := uuid.NewString()
	reservation := uuid.NewString()
	cases := map[string]any{
		"order.created":     contracts.OrderCreated{OrderID: order, CustomerID: "customer", AmountCents: 100, Items: []contracts.Item{{SKU: "SKU-001", Quantity: 1}}},
		"order.cancelled":   contracts.OrderCancelled{OrderID: order, Reason: "payment_failed"},
		"payment.processed": contracts.PaymentProcessed{PaymentID: payment, OrderID: order, AmountCents: 100, ChargedAt: now},
		"payment.failed":    contracts.PaymentFailed{PaymentID: payment, OrderID: order, Reason: "approval_limit_exceeded"},
		"payment.refunded":  contracts.PaymentRefunded{PaymentID: payment, OrderID: order, AmountCents: 100, RefundedAt: now},
		"stock.reserved":    contracts.StockReserved{ReservationID: reservation, OrderID: order, Items: []contracts.Item{{SKU: "SKU-001", Quantity: 1}}},
		"stock.failed":      contracts.StockFailed{OrderID: order, Reason: "insufficient_stock", UnavailableSKUs: []string{"SKU-001"}},
		"stock.released":    contracts.StockReleased{ReservationID: reservation, OrderID: order, ReleasedAt: now},
		"order.fulfilled":   contracts.OrderFulfilled{FulfillmentID: uuid.NewString(), OrderID: order, DispatchedAt: now},
	}
	loader := schema.NewLoader()
	require.NoError(t, loader.Load(context.Background()))
	for eventType, payload := range cases {
		t.Run(eventType, func(t *testing.T) {
			body, err := json.Marshal(payload)
			require.NoError(t, err)
			envelope, err := json.Marshal(client.Event{EventID: uuid.MustParse(id), EventType: eventType, CorrelationID: order, Payload: body, OccurredAt: now})
			require.NoError(t, err)
			require.NoError(t, loader.Validate(context.Background(), eventType, envelope))
		})
	}
}

func TestLifecycleTimestampsAreRequiredByCanonicalSchemas(t *testing.T) {
	now := time.Now().UTC()
	orderID := uuid.NewString()
	cases := map[string]map[string]any{
		"payment.processed": {
			"payment_id": uuid.NewString(), "order_id": orderID, "amount_cents": 100,
		},
		"payment.refunded": {
			"payment_id": uuid.NewString(), "order_id": orderID, "amount_cents": 100,
		},
		"stock.released": {
			"reservation_id": uuid.NewString(), "order_id": orderID,
		},
		"order.fulfilled": {
			"fulfillment_id": uuid.NewString(), "order_id": orderID,
		},
	}

	loader := schema.NewLoader()
	require.NoError(t, loader.Load(context.Background()))
	for eventType, payload := range cases {
		t.Run(eventType, func(t *testing.T) {
			body, err := json.Marshal(payload)
			require.NoError(t, err)
			envelope, err := json.Marshal(client.Event{
				EventID: uuid.New(), EventType: eventType, CorrelationID: orderID,
				Payload: body, OccurredAt: now,
			})
			require.NoError(t, err)
			require.Error(t, loader.Validate(context.Background(), eventType, envelope))
		})
	}
}
