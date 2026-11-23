// Package consumer implementa o consumer Kafka do gateway. Ele lê os tópicos
// de todos os event_type declarados em subscriptions, faz o fan-out por
// subscriber e grava uma linha em dispatch_outbox por par (event_id,
// service_id), com ON CONFLICT DO NOTHING. Assim, rebalance ou reset do
// consumer group não duplica nada. O offset só é commitado depois do COMMIT no
// banco: se algo falhar no meio, a mensagem é reprocessada.
//
// O payload de dispatch_outbox é o wrapper DispatchEnvelope, não o envelope
// cru. É assim que service_id e callback_url chegam ao HTTPSender sem mexer em
// shared/outbox.Row.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/gateway/internal/delivery"
	"github.com/victorvcruz/event-gateway/gateway/internal/observability"
	"github.com/victorvcruz/event-gateway/gateway/internal/repository"
	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
	"github.com/victorvcruz/event-gateway/shared/failpoint"
)

//go:generate mockgen -source=$GOFILE -destination=./mocks/mock_registry.go -package=mocks

type Subscriber struct {
	ID          string
	CallbackURL string
}

type SubscriptionRegistry interface {
	SubscriptionsOf(eventType string) []Subscriber
}

// wireEnvelope é o body de POST /events e, portanto, o formato das mensagens
// no Kafka. No wire o campo é "timestamp"; no Go chama OccurredAt.
type wireEnvelope struct {
	EventID       uuid.UUID       `json:"event_id"`
	EventType     string          `json:"event_type"`
	CorrelationID string          `json:"correlation_id"`
	Payload       json.RawMessage `json:"payload"`
	OccurredAt    time.Time       `json:"timestamp"`
}

type Metrics struct {
	Consumed *prometheus.CounterVec // labels: event_type, outcome
}

type Config struct {
	Brokers        []string
	GroupID        string
	SessionTimeout time.Duration
	EventTypes     []string
	Probe          failpoint.Probe
}

type KafkaConsumer struct {
	cfg        Config
	registry   SubscriptionRegistry
	pool       *pgxpool.Pool
	dispatch   *repository.Dispatch
	validator  schema.Validator
	deadLetter *repository.DeadLetter
	metrics    Metrics
	log        *zap.Logger

	client *kgo.Client
	admin  *kadm.Client

	lagMu sync.RWMutex
	lag   map[topicPart]int64
}

type topicPart struct {
	topic string
	part  int32
}

func New(cfg Config, registry SubscriptionRegistry, pool *pgxpool.Pool,
	dispatch *repository.Dispatch, validator schema.Validator, deadLetter *repository.DeadLetter,
	m Metrics, log *zap.Logger, extraOpts ...kgo.Opt,
) (*KafkaConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("consumer: brokers required")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("consumer: group_id required")
	}
	if len(cfg.EventTypes) == 0 {
		return nil, errors.New("consumer: at least one event_type required")
	}
	if validator == nil {
		return nil, errors.New("consumer: schema validator required")
	}
	if deadLetter == nil {
		return nil, errors.New("consumer: dead-letter repository required")
	}
	if log == nil {
		log = zap.NewNop()
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.EventTypes...),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	if cfg.SessionTimeout > 0 {
		opts = append(opts, kgo.SessionTimeout(cfg.SessionTimeout))
	}
	opts = append(opts, extraOpts...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}

	return &KafkaConsumer{
		cfg:        cfg,
		registry:   registry,
		pool:       pool,
		dispatch:   dispatch,
		validator:  validator,
		deadLetter: deadLetter,
		metrics:    m,
		log:        log,
		client:     client,
		admin:      kadm.NewClient(client),
		lag:        make(map[topicPart]int64),
	}, nil
}

func (c *KafkaConsumer) Close() { c.client.Close() }

// Run bloqueia até o ctx ser cancelado. A cada Poll, processa as mensagens
// numa transação, commita e só então commita o offset no Kafka. A entrega é
// at-least-once, e as duplicatas caem na constraint composta de dispatch_outbox.
func (c *KafkaConsumer) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 && ctx.Err() == nil {
			for _, e := range errs {
				c.log.Warn("kafka fetch error",
					zap.String("topic", e.Topic),
					zap.Int32("partition", e.Partition),
					zap.Error(e.Err))
			}
		}
		if fetches.NumRecords() == 0 {
			continue
		}

		records := fetches.Records()
		for {
			err := c.processBatch(ctx, records)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.log.Warn("process batch failed — offsets not committed, retrying same records", zap.Error(err))
			timer := time.NewTimer(500 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}

		if c.cfg.Probe != nil {
			for _, record := range records {
				var env wireEnvelope
				if json.Unmarshal(record.Value, &env) != nil {
					continue
				}
				c.cfg.Probe.Hit(ctx, failpoint.Checkpoint{
					Point:         failpoint.KafkaConsumerAfterDBCommit,
					EventID:       env.EventID.String(),
					EventType:     env.EventType,
					CorrelationID: env.CorrelationID,
				})
			}
		}

		if err := c.client.CommitRecords(ctx, records...); err != nil && ctx.Err() == nil {
			c.log.Warn("commit records failed", zap.Error(err))
		}
	}
}

func (c *KafkaConsumer) processBatch(ctx context.Context, records []*kgo.Record) error {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, r := range records {
		if err := c.processRecord(ctx, tx, r); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (c *KafkaConsumer) processRecord(ctx context.Context, tx pgx.Tx, r *kgo.Record) error {
	if !json.Valid(r.Value) {
		err := errors.New("payload is not valid JSON")
		c.log.Warn("malformed kafka payload",
			zap.String("topic", r.Topic), zap.Int64("offset", r.Offset), zap.Error(err))
		c.observe(r.Topic, "malformed")
		return c.deadLetterInvalid(ctx, tx, r, uuid.Nil, r.Topic, "malformed_json", err)
	}
	var env wireEnvelope
	if err := json.Unmarshal(r.Value, &env); err != nil {
		c.log.Warn("invalid kafka envelope",
			zap.String("topic", r.Topic), zap.Int64("offset", r.Offset), zap.Error(err))
		c.observe(r.Topic, "schema_invalid")
		return c.deadLetterInvalid(ctx, tx, r, uuid.Nil, r.Topic, "schema_invalid", err)
	}

	if env.EventType != "" && env.EventType != r.Topic {
		err := fmt.Errorf("event_type %q does not match topic %q", env.EventType, r.Topic)
		c.observe(r.Topic, "topic_mismatch")
		return c.deadLetterInvalid(ctx, tx, r, env.EventID, r.Topic, "topic_mismatch", err)
	}
	eventType := env.EventType
	if eventType == "" {
		eventType = r.Topic
	}
	if err := c.validator.Validate(ctx, eventType, r.Value); err != nil {
		switch {
		case errors.Is(err, schema.ErrSchemaNotFound):
			c.observe(env.EventType, "schema_not_found")
			return c.deadLetterInvalid(ctx, tx, r, env.EventID, env.EventType, "schema_not_found", err)
		case errors.Is(err, schema.ErrSchemaViolation):
			c.observe(env.EventType, "schema_invalid")
			return c.deadLetterInvalid(ctx, tx, r, env.EventID, env.EventType, "schema_invalid", err)
		default:
			c.observe(env.EventType, "registry_error")
			return fmt.Errorf("validate kafka schema: %w", err)
		}
	}
	if env.EventID == uuid.Nil {
		err := errors.New("event_id must be a non-zero UUID")
		c.observe(eventType, "schema_invalid")
		return c.deadLetterInvalid(ctx, tx, r, uuid.Nil, eventType, "schema_invalid", err)
	}

	subs := c.registry.SubscriptionsOf(env.EventType)
	if len(subs) == 0 {
		c.log.Info("kafka message with no subscribers — skip",
			zap.String("event_type", env.EventType),
			zap.String("event_id", env.EventID.String()),
			zap.Int64("offset", r.Offset))
		c.observe(env.EventType, "no_subscribers")
		return nil
	}

	anyInserted := false
	for _, s := range subs {
		wrapped, err := delivery.EncodeDispatchEnvelope(
			delivery.TargetMeta{ServiceID: s.ID, CallbackURL: s.CallbackURL},
			r.Value,
		)
		if err != nil {
			return fmt.Errorf("encode wrapper for %s: %w", s.ID, err)
		}
		inserted, err := c.dispatch.InsertPending(ctx, tx, repository.DispatchRow{
			EventID:       env.EventID,
			ServiceID:     s.ID,
			CallbackURL:   s.CallbackURL,
			EventType:     env.EventType,
			CorrelationID: env.CorrelationID,
			Payload:       json.RawMessage(wrapped),
			OccurredAt:    env.OccurredAt,
		})
		if err != nil {
			return fmt.Errorf("insert dispatch row: %w", err)
		}
		if inserted {
			anyInserted = true
		}
	}
	subIDs := make([]string, 0, len(subs))
	for _, s := range subs {
		subIDs = append(subIDs, s.ID)
	}
	if anyInserted {
		c.log.Info("kafka message fanned out",
			zap.String("event_type", env.EventType),
			zap.String("event_id", env.EventID.String()),
			zap.String("correlation_id", env.CorrelationID),
			zap.Int("subscribers", len(subs)),
			zap.Strings("subscriber_ids", subIDs),
			zap.Int64("offset", r.Offset))
		c.observe(env.EventType, "persisted")
	} else {
		c.log.Info("kafka message already dispatched — skip (duplicate)",
			zap.String("event_type", env.EventType),
			zap.String("event_id", env.EventID.String()),
			zap.Int64("offset", r.Offset))
		c.observe(env.EventType, "duplicate")
	}
	return nil
}

func (c *KafkaConsumer) deadLetterInvalid(ctx context.Context, tx pgx.Tx, record *kgo.Record,
	eventID uuid.UUID, eventType, code string, cause error,
) error {
	inserted, err := c.deadLetter.InsertKafkaInvalid(ctx, tx, repository.KafkaInvalidRow{
		EventID: eventID, EventType: eventType, RawPayload: record.Value,
		FailureCode: code, FailureReason: cause.Error(), SourceTopic: record.Topic,
		SourcePartition: record.Partition, SourceOffset: record.Offset,
	})
	if err != nil {
		return fmt.Errorf("insert invalid kafka event into dead_letter: %w", err)
	}
	if inserted {
		c.log.Warn("invalid kafka event moved to dead_letter",
			zap.String("failure_code", code), zap.String("topic", record.Topic),
			zap.Int32("partition", record.Partition), zap.Int64("offset", record.Offset))
	}
	return nil
}

func (c *KafkaConsumer) observe(eventType, outcome string) {
	if c.metrics.Consumed == nil {
		return
	}
	c.metrics.Consumed.WithLabelValues(eventType, outcome).Inc()
}

// Snapshot implementa observability.KafkaLagReporter e devolve o lag por
// (topic, partition). O refreshLag preenche em segundo plano, então pode vir
// vazio nos primeiros ciclos.
func (c *KafkaConsumer) Snapshot() []observability.KafkaLag {
	c.lagMu.RLock()
	defer c.lagMu.RUnlock()
	out := make([]observability.KafkaLag, 0, len(c.lag))
	for k, v := range c.lag {
		out = append(out, observability.KafkaLag{Topic: k.topic, Partition: k.part, Lag: v})
	}
	return out
}

// StartLagReporter atualiza o lag do consumer group de tempos em tempos via
// kadm.Client.Lag. Se uma consulta falhar, loga e segue; a métrica fica sem
// dados por um tempo.
func (c *KafkaConsumer) StartLagReporter(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.refreshLag(ctx)
		}
	}
}

func (c *KafkaConsumer) refreshLag(ctx context.Context) {
	lag, err := c.admin.Lag(ctx, c.cfg.GroupID)
	if err != nil {
		c.log.Warn("kafka lag lookup failed", zap.Error(err))
		return
	}
	snap := make(map[topicPart]int64)
	for _, group := range lag {
		for _, topic := range group.Lag {
			for _, part := range topic {
				snap[topicPart{topic: part.Commit.Topic, part: part.Commit.Partition}] = part.Lag
			}
		}
	}
	c.lagMu.Lock()
	c.lag = snap
	c.lagMu.Unlock()
}
