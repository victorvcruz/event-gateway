package app

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
	"github.com/victorvcruz/event-gateway/shared/contracts"
)

func TestExtractRejectsInvalidOrderID(t *testing.T) {
	payload, err := json.Marshal(contracts.OrderFulfilled{
		FulfillmentID: uuid.NewString(), OrderID: "not-a-uuid", DispatchedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err = extract(dispatcher.Event{EventType: "order.fulfilled", Payload: payload}); err == nil {
		t.Fatal("expected invalid order_id to be rejected")
	}
}
