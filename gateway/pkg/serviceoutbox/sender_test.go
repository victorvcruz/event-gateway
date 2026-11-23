package serviceoutbox_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/victorvcruz/event-gateway/gateway/pkg/client"
	"github.com/victorvcruz/event-gateway/gateway/pkg/serviceoutbox"
	"github.com/victorvcruz/event-gateway/shared/contracts"
	"github.com/victorvcruz/event-gateway/shared/outbox"
)

func TestSenderClassifiesGatewayResponses(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		nonRetryable bool
	}{
		{"accepted", http.StatusAccepted, false},
		{"duplicate", http.StatusConflict, false},
		{"validation", http.StatusUnprocessableEntity, true},
		{"unavailable", http.StatusServiceUnavailable, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(tt.status) }))
			defer srv.Close()
			gateway, err := client.New(srv.URL)
			require.NoError(t, err)
			sender, err := serviceoutbox.New(gateway)
			require.NoError(t, err)
			row, err := serviceoutbox.NewRow("order.created", "saga-1", contracts.OrderCreated{OrderID: "6c8a1d7f-6e3f-4f13-9b9a-1a4b1e7c0e88", CustomerID: "c", AmountCents: 1, Items: []contracts.Item{{SKU: "SKU-001", Quantity: 1}}})
			require.NoError(t, err)
			err = sender.Send(context.Background(), row)
			if tt.status == http.StatusAccepted || tt.status == http.StatusConflict {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Equal(t, tt.nonRetryable, errors.Is(err, outbox.ErrNonRetryable))
		})
	}
}

func TestSenderRejectsEnvelopeColumnMismatch(t *testing.T) {
	gateway, _ := client.New("http://gateway.invalid")
	sender, _ := serviceoutbox.New(gateway)
	row, _ := serviceoutbox.NewRow("order.cancelled", "saga", contracts.OrderCancelled{OrderID: "6c8a1d7f-6e3f-4f13-9b9a-1a4b1e7c0e88", Reason: "payment_failed"})
	row.EventType = "stock.failed"
	err := sender.Send(context.Background(), row)
	require.ErrorIs(t, err, outbox.ErrNonRetryable)
}
