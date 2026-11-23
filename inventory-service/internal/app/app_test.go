package app

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
	"github.com/victorvcruz/event-gateway/shared/contracts"
)

func TestOrderCreatedRejectsNonPositiveQuantityBeforeDatabase(t *testing.T) {
	payload, err := json.Marshal(contracts.OrderCreated{
		OrderID: uuid.NewString(), CustomerID: "customer", AmountCents: 100,
		Items: []contracts.Item{{SKU: "SKU-001", Quantity: -1}},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = (&App{}).orderCreated(context.Background(), dispatcher.Event{Payload: payload})
	if !errors.Is(err, dispatcher.ErrBadRequest) {
		t.Fatalf("expected ErrBadRequest, got %v", err)
	}
}
