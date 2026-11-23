// Package publisher implementa o outbox.Sender do gateway sobre franz-go.
// A key da mensagem é sempre o correlation_id e o acks é all.
package publisher

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/shared/outbox"
)

type KafkaSender struct {
	client *kgo.Client
	log    *zap.Logger
}

func NewKafkaSender(brokers []string, extraOpts ...kgo.Opt) (*KafkaSender, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerBatchMaxBytes(1 << 20),
	}
	opts = append(opts, extraOpts...)

	c, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("open kafka client: %w", err)
	}
	return &KafkaSender{client: c, log: zap.NewNop()}, nil
}

// WithLogger define o logger. Sem ele, o sender não loga nada.
func (s *KafkaSender) WithLogger(l *zap.Logger) *KafkaSender {
	if l != nil {
		s.log = l
	}
	return s
}

func (s *KafkaSender) Close() { s.client.Close() }

// Send publica a Row no tópico row.EventType, com row.CorrelationID como key,
// e espera o ACK do broker.
func (s *KafkaSender) Send(ctx context.Context, row outbox.Row) error {
	rec := &kgo.Record{
		Topic: row.EventType,
		Key:   []byte(row.CorrelationID),
		Value: []byte(row.Payload),
	}
	start := time.Now()
	if err := s.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		s.log.Warn("kafka publish failed",
			zap.String("event_type", row.EventType),
			zap.String("event_id", row.EventID.String()),
			zap.String("correlation_id", row.CorrelationID),
			zap.Duration("latency", time.Since(start)),
			zap.Error(err))
		return fmt.Errorf("produce %s: %w", row.EventType, err)
	}
	s.log.Info("kafka publish ack",
		zap.String("event_type", row.EventType),
		zap.String("event_id", row.EventID.String()),
		zap.String("correlation_id", row.CorrelationID),
		zap.Duration("latency", time.Since(start)))
	return nil
}
