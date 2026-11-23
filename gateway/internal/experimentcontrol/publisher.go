package experimentcontrol

import (
	"context"
	"encoding/json"

	"github.com/victorvcruz/event-gateway/gateway/pkg/client"
	"github.com/victorvcruz/event-gateway/shared/outbox"
)

type KafkaSender interface {
	Send(context.Context, outbox.Row) error
}

type KafkaPublisher struct{ sender KafkaSender }

func NewKafkaPublisher(sender KafkaSender) *KafkaPublisher {
	return &KafkaPublisher{sender: sender}
}

func (p *KafkaPublisher) Publish(ctx context.Context, event client.Event) error {
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	result := make(chan error, 1)
	go func() {
		result <- p.sender.Send(ctx, outbox.Row{
			EventID: event.EventID, EventType: event.EventType,
			CorrelationID: event.CorrelationID, Payload: raw,
		})
	}()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		// Mesmo com o broker recém-caído travando a resolução de metadata, a
		// requisição HTTP precisa ter prazo.
		return ctx.Err()
	}
}
