package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
	"github.com/victorvcruz/event-gateway/shared/contracts"
)

func TestPostOrderRejectsTrailingJSONBeforeDatabase(t *testing.T) {
	body := []byte(`{"customer_id":"customer","amount_cents":100,"items":[{"sku":"SKU-001","quantity":1}]} {"unexpected":true}`)
	req := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewReader(body))
	req.Header.Set("Idempotency-Key", "trailing-json")
	res := httptest.NewRecorder()

	(&App{}).postOrder(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestPaymentFailedRejectsInvalidOrderIDBeforeDatabase(t *testing.T) {
	payload, err := json.Marshal(contracts.PaymentFailed{
		PaymentID: uuid.NewString(), OrderID: "not-a-uuid", Reason: "declined",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = (&App{}).handlePaymentFailed(context.Background(), dispatcher.Event{Payload: payload})
	if !errors.Is(err, dispatcher.ErrBadRequest) {
		t.Fatalf("expected ErrBadRequest, got %v", err)
	}
}
