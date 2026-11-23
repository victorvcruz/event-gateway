package contracts_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/victorvcruz/event-gateway/shared/contracts"
)

func TestOrderCreatedRejectsInvalidItems(t *testing.T) {
	base := contracts.OrderCreated{OrderID: uuid.NewString(), CustomerID: "customer", AmountCents: 100}
	tests := []struct {
		name  string
		items []contracts.Item
	}{
		{"empty", nil},
		{"negative quantity", []contracts.Item{{SKU: "SKU-001", Quantity: -1}}},
		{"zero quantity", []contracts.Item{{SKU: "SKU-001", Quantity: 0}}},
		{"empty sku", []contracts.Item{{SKU: " ", Quantity: 1}}},
		{"duplicate sku", []contracts.Item{{SKU: "SKU-001", Quantity: 1}, {SKU: "SKU-001", Quantity: 1}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base.Items = test.items
			if err := contracts.ValidateOrderCreated(base); !errors.Is(err, contracts.ErrInvalidPayload) {
				t.Fatalf("expected invalid payload, got %v", err)
			}
		})
	}
}

func TestRequiredIdentifiersAreUUIDs(t *testing.T) {
	orderID := uuid.NewString()
	if err := contracts.ValidatePaymentProcessed(contracts.PaymentProcessed{OrderID: orderID, PaymentID: "", AmountCents: 1}); err == nil {
		t.Fatal("missing payment_id accepted")
	}
	if err := contracts.ValidateStockReserved(contracts.StockReserved{OrderID: orderID, ReservationID: "bad", Items: []contracts.Item{{SKU: "SKU-001", Quantity: 1}}}); err == nil {
		t.Fatal("invalid reservation_id accepted")
	}
	if err := contracts.ValidateOrderFulfilled(contracts.OrderFulfilled{OrderID: "bad", FulfillmentID: uuid.NewString()}); err == nil {
		t.Fatal("invalid order_id accepted")
	}
}

func TestDecodePayloadRejectsUnknownAndTrailingData(t *testing.T) {
	for _, raw := range []string{
		`{"order_id":"` + uuid.NewString() + `","reason":"payment_failed","unexpected":true}`,
		`{"order_id":"` + uuid.NewString() + `","reason":"payment_failed"} {}`,
	} {
		var payload contracts.OrderCancelled
		if err := contracts.DecodePayload([]byte(raw), &payload); !errors.Is(err, contracts.ErrInvalidPayload) {
			t.Fatalf("expected invalid payload for %q, got %v", raw, err)
		}
	}
}
