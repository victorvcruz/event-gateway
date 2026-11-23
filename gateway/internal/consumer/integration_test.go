package consumer_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/gateway/internal/consumer"
	"github.com/victorvcruz/event-gateway/gateway/internal/repository"
	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
	gwmigrations "github.com/victorvcruz/event-gateway/gateway/migrations"
	sharedmig "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
)

type env struct {
	pool     *pgxpool.Pool
	brokers  []string
	producer *kgo.Client
	consumed *prometheus.CounterVec
	teardown func()
}

type acceptValidator struct{}

func (acceptValidator) Validate(context.Context, string, json.RawMessage) error { return nil }

func startEnv(t *testing.T, topics ...string) *env {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pgContainer, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("gateway_db"),
		tcpostgres.WithUsername("gateway"),
		tcpostgres.WithPassword("gateway"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(30*time.Second),
		),
	)
	require.NoError(t, err)

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	require.NoError(t, sharedmig.Apply(dsn))
	require.NoError(t, sharedmig.Apply(dsn, sharedmig.WithTableName("dispatch_outbox")))
	require.NoError(t, gwmigrations.Apply(dsn))

	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)

	kafkaContainer, err := tckafka.Run(ctx,
		"confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("tcc-poc"),
	)
	require.NoError(t, err)

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)

	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	if len(topics) > 0 {
		_, err = kadm.NewClient(admin).CreateTopics(ctx, 3, 1, nil, topics...)
		require.NoError(t, err)
	}

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(1<<20),
	)
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	consumed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_dispatch_consumed_total",
	}, []string{"event_type", "outcome"})
	reg.MustRegister(consumed)

	return &env{
		pool: pool, brokers: brokers, producer: producer, consumed: consumed,
		teardown: func() {
			producer.Close()
			admin.Close()
			pool.Close()
			_ = kafkaContainer.Terminate(context.Background())
			_ = pgContainer.Terminate(context.Background())
		},
	}
}

func counterVal(t *testing.T, cv *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, cv.WithLabelValues(labels...).Write(m))
	return m.GetCounter().GetValue()
}

func newConsumer(t *testing.T, e *env, reg consumer.SubscriptionRegistry, topics ...string) (*consumer.KafkaConsumer, func()) {
	return newConsumerWithValidator(t, e, reg, acceptValidator{}, topics...)
}

func newConsumerWithValidator(t *testing.T, e *env, reg consumer.SubscriptionRegistry, validator schema.Validator, topics ...string) (*consumer.KafkaConsumer, func()) {
	t.Helper()
	c, err := consumer.New(consumer.Config{
		Brokers:        e.brokers,
		GroupID:        "gateway-dispatcher-" + uuid.NewString(),
		SessionTimeout: 6 * time.Second,
		EventTypes:     topics,
	}, reg, e.pool, repository.NewDispatch(e.pool), validator, repository.NewDeadLetter(e.pool),
		consumer.Metrics{Consumed: e.consumed}, zap.NewNop())
	require.NoError(t, err)
	return c, c.Close
}

func produce(t *testing.T, e *env, topic string, body []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := e.producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: body}).FirstErr()
	require.NoError(t, err)
}

func waitForRows(t *testing.T, pool *pgxpool.Pool, eventID uuid.UUID, want int) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM dispatch_outbox WHERE event_id=$1`, eventID).Scan(&n))
		if n == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("dispatch_outbox nunca alcançou %d linhas para event_id=%s", want, eventID)
}

func TestConsumerFanOutToTwoSubscribers(t *testing.T) {
	e := startEnv(t, "order.created")
	defer e.teardown()

	reg := stubRegistryPub{
		"order.created": {
			{ID: "payment-service", CallbackURL: "http://payment/dispatch"},
			{ID: "inventory-service", CallbackURL: "http://inventory/dispatch"},
		},
	}
	c, close := newConsumer(t, e, reg, "order.created")
	defer close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	eventID := uuid.New()
	body, _ := json.Marshal(map[string]any{
		"event_id":       eventID,
		"event_type":     "order.created",
		"correlation_id": "saga-t01",
		"payload":        map[string]any{"order_id": "x"},
		"timestamp":      time.Now().UTC(),
	})
	produce(t, e, "order.created", body)

	waitForRows(t, e.pool, eventID, 2)
	assert.Equal(t, float64(1), counterVal(t, e.consumed, "order.created", "persisted"))
}

func TestConsumerNoSubscribersMetricIncrements(t *testing.T) {
	e := startEnv(t, "order.created")
	defer e.teardown()

	reg := stubRegistryPub{} // vazio
	c, close := newConsumer(t, e, reg, "order.created")
	defer close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	body, _ := json.Marshal(map[string]any{
		"event_id":       uuid.New(),
		"event_type":     "order.created",
		"correlation_id": "saga-t10",
		"payload":        map[string]any{},
		"timestamp":      time.Now().UTC(),
	})
	produce(t, e, "order.created", body)

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if counterVal(t, e.consumed, "order.created", "no_subscribers") >= 1 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	assert.GreaterOrEqual(t,
		counterVal(t, e.consumed, "order.created", "no_subscribers"), float64(1),
		"métrica no_subscribers deve incrementar")

	var n int
	require.NoError(t, e.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM dispatch_outbox`).Scan(&n))
	assert.Equal(t, 0, n, "sem subscribers → zero linhas em dispatch_outbox")
}

func TestConsumerMalformedPayloadDoesNotStall(t *testing.T) {
	e := startEnv(t, "order.created")
	defer e.teardown()

	reg := stubRegistryPub{
		"order.created": {{ID: "x", CallbackURL: "http://x/dispatch"}},
	}
	c, close := newConsumer(t, e, reg, "order.created")
	defer close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	// primeiro: bytes não-JSON
	produce(t, e, "order.created", []byte("this is not json"))

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if counterVal(t, e.consumed, "order.created", "malformed") >= 1 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	assert.GreaterOrEqual(t,
		counterVal(t, e.consumed, "order.created", "malformed"), float64(1))
	var deadLetters int
	var raw []byte
	var code string
	require.NoError(t, e.pool.QueryRow(context.Background(), `SELECT count(*) FROM dead_letter WHERE failure_code='malformed_json'`).Scan(&deadLetters))
	require.Equal(t, 1, deadLetters)
	require.NoError(t, e.pool.QueryRow(context.Background(), `SELECT raw_payload, failure_code FROM dead_letter WHERE failure_code='malformed_json'`).Scan(&raw, &code))
	assert.Equal(t, []byte("this is not json"), raw)
	assert.Equal(t, "malformed_json", code)

	// prova que o consumer não travou: envio válido seguinte é processado normalmente
	eventID := uuid.New()
	body, _ := json.Marshal(map[string]any{
		"event_id":       eventID,
		"event_type":     "order.created",
		"correlation_id": "saga-t11",
		"payload":        map[string]any{},
		"timestamp":      time.Now().UTC(),
	})
	produce(t, e, "order.created", body)
	waitForRows(t, e.pool, eventID, 1)
}

type recoveringValidator struct{ available atomic.Bool }

func (v *recoveringValidator) Validate(context.Context, string, json.RawMessage) error {
	if !v.available.Load() {
		return schema.ErrRegistryUnavailable
	}
	return nil
}

func TestRegistryRecoveryProcessesPreviouslyUncommittedKafkaRecord(t *testing.T) {
	e := startEnv(t, "order.created")
	defer e.teardown()
	validator := &recoveringValidator{}
	reg := stubRegistryPub{"order.created": {{ID: "payment", CallbackURL: "http://payment/dispatch"}}}
	c, close := newConsumerWithValidator(t, e, reg, validator, "order.created")
	defer close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	eventID := uuid.New()
	body, _ := json.Marshal(map[string]any{
		"event_id": eventID, "event_type": "order.created", "correlation_id": uuid.NewString(),
		"payload": map[string]any{}, "timestamp": time.Now().UTC(),
	})
	produce(t, e, "order.created", body)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && counterVal(t, e.consumed, "order.created", "registry_error") == 0 {
		time.Sleep(100 * time.Millisecond)
	}
	assert.Greater(t, counterVal(t, e.consumed, "order.created", "registry_error"), float64(0))
	var count int
	require.NoError(t, e.pool.QueryRow(context.Background(), `SELECT count(*) FROM dispatch_outbox`).Scan(&count))
	assert.Zero(t, count)
	require.NoError(t, e.pool.QueryRow(context.Background(), `SELECT count(*) FROM dead_letter`).Scan(&count))
	assert.Zero(t, count)

	validator.available.Store(true)
	waitForRows(t, e.pool, eventID, 1)
}

// TestKafkaReplayAbsorbedByCompositePK: publicar a mesma mensagem duas vezes
// (mesmo event_id) deixa N linhas em dispatch_outbox, uma por subscriber, e não
// 2N. A reentrega cai na constraint composta via ON CONFLICT DO NOTHING.
func TestKafkaReplayAbsorbedByCompositePK(t *testing.T) {
	e := startEnv(t, "order.created")
	defer e.teardown()

	reg := stubRegistryPub{
		"order.created": {
			{ID: "payment-service", CallbackURL: "http://payment/dispatch"},
			{ID: "inventory-service", CallbackURL: "http://inventory/dispatch"},
		},
	}
	c, close := newConsumer(t, e, reg, "order.created")
	defer close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	eventID := uuid.New()
	body, _ := json.Marshal(map[string]any{
		"event_id":       eventID,
		"event_type":     "order.created",
		"correlation_id": "saga-t02",
		"payload":        map[string]any{"order_id": "y"},
		"timestamp":      time.Now().UTC(),
	})
	// Publica DUAS vezes — simula reentrega Kafka (rebalance, offset reset).
	produce(t, e, "order.created", body)
	produce(t, e, "order.created", body)

	// Espera até vermos ambos: 2 linhas (constraint absorve a segunda mensagem)
	// E o outcome=duplicate incrementado.
	deadline := time.Now().Add(30 * time.Second)
	var rowCount int
	for time.Now().Before(deadline) {
		require.NoError(t, e.pool.QueryRow(context.Background(),
			`SELECT count(*) FROM dispatch_outbox WHERE event_id=$1`, eventID).Scan(&rowCount))
		if rowCount == 2 && counterVal(t, e.consumed, "order.created", "duplicate") >= 1 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	assert.Equal(t, 2, rowCount,
		"exatamente 2 linhas (uma por subscriber), NÃO 4 — constraint composta absorve reentrega")

	assert.GreaterOrEqual(t,
		counterVal(t, e.consumed, "order.created", "persisted"), float64(1),
		"primeira ingestão contabilizada como persisted")
	assert.GreaterOrEqual(t,
		counterVal(t, e.consumed, "order.created", "duplicate"), float64(1),
		"reentrega contabilizada como duplicate")
}

type stubRegistryPub map[string][]consumer.Subscriber

func (s stubRegistryPub) SubscriptionsOf(eventType string) []consumer.Subscriber {
	return s[eventType]
}
