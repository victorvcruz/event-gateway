package app

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
	"github.com/victorvcruz/event-gateway/shared/contracts"
)

func TestPaymentRejectsMissingPaymentIDBeforeDatabase(t *testing.T) {
	payload, err := json.Marshal(contracts.PaymentProcessed{
		OrderID: uuid.NewString(), AmountCents: 100, ChargedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}

	err = (&App{}).payment(context.Background(), dispatcher.Event{Payload: payload})
	if !errors.Is(err, dispatcher.ErrBadRequest) {
		t.Fatalf("expected ErrBadRequest, got %v", err)
	}
}

func TestStockRejectsMissingReservationIDBeforeDatabase(t *testing.T) {
	payload, err := json.Marshal(contracts.StockReserved{
		OrderID: uuid.NewString(), Items: []contracts.Item{{SKU: "SKU-001", Quantity: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = (&App{}).stock(context.Background(), dispatcher.Event{Payload: payload})
	if !errors.Is(err, dispatcher.ErrBadRequest) {
		t.Fatalf("expected ErrBadRequest, got %v", err)
	}
}
