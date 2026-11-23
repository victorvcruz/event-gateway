// Package serviceoutbox liga o outbox de um serviço de negócio ao cliente HTTP
// do gateway. O payload precisa ser um JSON completo de client.Event.
package serviceoutbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/victorvcruz/event-gateway/gateway/pkg/client"
	"github.com/victorvcruz/event-gateway/shared/outbox"
)

type Sender struct{ gateway *client.Client }

// NewRow monta o envelope e o guarda intacto na linha do outbox.
func NewRow(eventType, correlationID string, payload any) (outbox.Row, error) {
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return outbox.Row{}, fmt.Errorf("marshal payload: %w", err)
	}
	event := client.Event{EventID: uuid.New(), EventType: eventType, CorrelationID: correlationID, Payload: rawPayload, OccurredAt: time.Now().UTC()}
	rawEvent, err := json.Marshal(event)
	if err != nil {
		return outbox.Row{}, fmt.Errorf("marshal envelope: %w", err)
	}
	return outbox.Row{EventID: event.EventID, EventType: eventType, CorrelationID: correlationID, Payload: rawEvent}, nil
}

func New(gateway *client.Client) (*Sender, error) {
	if gateway == nil {
		return nil, errors.New("serviceoutbox: gateway client is required")
	}
	return &Sender{gateway: gateway}, nil
}

func (s *Sender) Send(ctx context.Context, row outbox.Row) error {
	var event client.Event
	if err := json.Unmarshal(row.Payload, &event); err != nil {
		return fmt.Errorf("%w: decode stored envelope: %v", outbox.ErrNonRetryable, err)
	}
	if event.EventID != row.EventID || event.EventType != row.EventType || event.CorrelationID != row.CorrelationID {
		return fmt.Errorf("%w: stored envelope does not match outbox columns", outbox.ErrNonRetryable)
	}
	_, err := s.gateway.PublishEvent(ctx, event)
	if err == nil {
		return nil
	}
	if errors.Is(err, client.ErrGatewayUnavailable) {
		return err
	}
	return fmt.Errorf("%w: %v", outbox.ErrNonRetryable, err)
}
