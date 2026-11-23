package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/gateway/internal/delivery"
	"github.com/victorvcruz/event-gateway/gateway/internal/repository"
	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
	gwmigrations "github.com/victorvcruz/event-gateway/gateway/migrations"
	sharedmig "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
)

type acceptValidator struct{}

func (acceptValidator) Validate(context.Context, string, json.RawMessage) error { return nil }

type validatorFunc func(context.Context, string, json.RawMessage) error

func (f validatorFunc) Validate(ctx context.Context, eventType string, raw json.RawMessage) error {
	return f(ctx, eventType, raw)
}

func startPostgres(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("gateway_db"),
		tcpostgres.WithUsername("gateway"),
		tcpostgres.WithPassword("gateway"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	require.NoError(t, err)

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	require.NoError(t, sharedmig.Apply(dsn))
	require.NoError(t, sharedmig.Apply(dsn, sharedmig.WithTableName("dispatch_outbox")))
	require.NoError(t, gwmigrations.Apply(dsn))

	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)

	return pool, func() {
		pool.Close()
		_ = container.Terminate(context.Background())
	}
}

type stubRegistry map[string][]Subscriber

func (s stubRegistry) SubscriptionsOf(eventType string) []Subscriber {
	return s[eventType]
}

func newTestConsumer(pool *pgxpool.Pool, reg SubscriptionRegistry) *KafkaConsumer {
	return &KafkaConsumer{
		cfg:        Config{GroupID: "test"},
		registry:   reg,
		pool:       pool,
		dispatch:   repository.NewDispatch(pool),
		validator:  acceptValidator{},
		deadLetter: repository.NewDeadLetter(pool),
		log:        zap.NewNop(),
	}
}

func newRecord(topic string, env wireEnvelope) *kgo.Record {
	body, _ := json.Marshal(env)
	return &kgo.Record{Topic: topic, Value: body}
}

func TestProcessRecordFansOutOverAllSubscribers(t *testing.T) {
	pool, teardown := startPostgres(t)
	defer teardown()

	reg := stubRegistry{
		"order.created": {
			{ID: "payment-service", CallbackURL: "http://payment/dispatch"},
			{ID: "inventory-service", CallbackURL: "http://inventory/dispatch"},
		},
	}
	c := newTestConsumer(pool, reg)

	env := wireEnvelope{
		EventID:       uuid.New(),
		EventType:     "order.created",
		CorrelationID: "saga-1",
		Payload:       json.RawMessage(`{"order_id":"x"}`),
		OccurredAt:    time.Now().UTC(),
	}
	rec := newRecord("order.created", env)

	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()

	require.NoError(t, c.processRecord(ctx, tx, rec))
	require.NoError(t, tx.Commit(ctx))

	rows, err := pool.Query(ctx,
		`SELECT service_id, callback_url, status FROM dispatch_outbox
		 WHERE event_id=$1 ORDER BY service_id`, env.EventID)
	require.NoError(t, err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var svc, url, status string
		require.NoError(t, rows.Scan(&svc, &url, &status))
		assert.Equal(t, "PENDING", status)
		got = append(got, svc+"@"+url)
	}
	assert.ElementsMatch(t, []string{
		"inventory-service@http://inventory/dispatch",
		"payment-service@http://payment/dispatch",
	}, got)
}

func TestMalformedKafkaReplayCreatesExactlyOneDeadLetterWithRawBytes(t *testing.T) {
	pool, teardown := startPostgres(t)
	defer teardown()
	c := newTestConsumer(pool, stubRegistry{})
	record := &kgo.Record{Topic: "order.created", Partition: 2, Offset: 31, Value: []byte("not-json")}

	for range 2 {
		tx, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
		require.NoError(t, err)
		require.NoError(t, c.processRecord(context.Background(), tx, record))
		require.NoError(t, tx.Commit(context.Background()))
	}

	var count int
	var eventID uuid.UUID
	var raw []byte
	var payload []byte
	var code string
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM dead_letter WHERE source_topic=$1 AND source_partition=$2 AND source_offset=$3
	`, record.Topic, record.Partition, record.Offset).Scan(&count))
	assert.Equal(t, 1, count)
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT event_id, raw_payload, payload::text, failure_code
		FROM dead_letter WHERE source_topic=$1 AND source_partition=$2 AND source_offset=$3
	`, record.Topic, record.Partition, record.Offset).Scan(&eventID, &raw, &payload, &code))
	assert.Equal(t, repository.KafkaInvalidEventID(record.Topic, record.Partition, record.Offset), eventID)
	assert.Equal(t, record.Value, raw)
	assert.Nil(t, payload)
	assert.Equal(t, "malformed_json", code)
}

func TestTopicMismatchCreatesDeadLetterWithoutDispatch(t *testing.T) {
	pool, teardown := startPostgres(t)
	defer teardown()
	c := newTestConsumer(pool, stubRegistry{"payment.failed": {{ID: "order", CallbackURL: "http://order/dispatch"}}})
	env := wireEnvelope{EventID: uuid.New(), EventType: "payment.failed", CorrelationID: uuid.NewString(), Payload: json.RawMessage(`{}`), OccurredAt: time.Now().UTC()}
	record := newRecord("order.created", env)
	record.Partition, record.Offset = 1, 7

	tx, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, c.processRecord(context.Background(), tx, record))
	require.NoError(t, tx.Commit(context.Background()))

	var code string
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT failure_code FROM dead_letter WHERE event_id=$1`, env.EventID).Scan(&code))
	assert.Equal(t, "topic_mismatch", code)
	var dispatches int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM dispatch_outbox WHERE event_id=$1`, env.EventID).Scan(&dispatches))
	assert.Zero(t, dispatches)
}

func TestSchemaViolationCreatesDeadLetterWithJSONPayload(t *testing.T) {
	pool, teardown := startPostgres(t)
	defer teardown()
	c := newTestConsumer(pool, stubRegistry{})
	c.validator = validatorFunc(func(context.Context, string, json.RawMessage) error {
		return schema.ErrSchemaViolation
	})
	env := wireEnvelope{EventID: uuid.New(), EventType: "order.created", CorrelationID: uuid.NewString(), Payload: json.RawMessage(`{}`), OccurredAt: time.Now().UTC()}
	record := newRecord("order.created", env)
	record.Partition, record.Offset = 0, 18

	tx, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, c.processRecord(context.Background(), tx, record))
	require.NoError(t, tx.Commit(context.Background()))

	var code string
	var payload []byte
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT failure_code, payload FROM dead_letter WHERE event_id=$1`, env.EventID).Scan(&code, &payload))
	assert.Equal(t, "schema_invalid", code)
	assert.JSONEq(t, string(record.Value), string(payload))
}

func TestRegistryUnavailableAbortsRecordWithoutDeadLetterOrDispatch(t *testing.T) {
	pool, teardown := startPostgres(t)
	defer teardown()
	c := newTestConsumer(pool, stubRegistry{"order.created": {{ID: "payment", CallbackURL: "http://payment/dispatch"}}})
	c.validator = validatorFunc(func(context.Context, string, json.RawMessage) error {
		return schema.ErrRegistryUnavailable
	})
	env := wireEnvelope{EventID: uuid.New(), EventType: "order.created", CorrelationID: uuid.NewString(), Payload: json.RawMessage(`{}`), OccurredAt: time.Now().UTC()}
	record := newRecord("order.created", env)
	record.Partition, record.Offset = 0, 9

	tx, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
	require.NoError(t, err)
	err = c.processRecord(context.Background(), tx, record)
	require.Error(t, err)
	assert.True(t, errors.Is(err, schema.ErrRegistryUnavailable))
	require.NoError(t, tx.Rollback(context.Background()))

	for _, table := range []string{"dead_letter", "dispatch_outbox"} {
		var count int
		require.NoError(t, pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&count)) //nolint:gosec
		assert.Zero(t, count)
	}
}

func TestProcessRecordPayloadIsWrapperNotRaw(t *testing.T) {
	pool, teardown := startPostgres(t)
	defer teardown()

	reg := stubRegistry{
		"order.created": {{ID: "payment-service", CallbackURL: "http://payment/dispatch"}},
	}
	c := newTestConsumer(pool, reg)

	env := wireEnvelope{
		EventID: uuid.New(), EventType: "order.created",
		CorrelationID: "saga-2",
		Payload:       json.RawMessage(`{"amount":9900}`),
		OccurredAt:    time.Now().UTC(),
	}
	rec := newRecord("order.created", env)

	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, c.processRecord(ctx, tx, rec))
	require.NoError(t, tx.Commit(ctx))

	var payload []byte
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT payload FROM dispatch_outbox WHERE event_id=$1`, env.EventID).Scan(&payload))

	w, err := delivery.DecodeDispatchEnvelope(payload)
	require.NoError(t, err, "gravado deve ser DispatchEnvelope válido")
	assert.Equal(t, "payment-service", w.Target.ServiceID)
	assert.Equal(t, "http://payment/dispatch", w.Target.CallbackURL)

	assert.JSONEq(t, string(rec.Value), string(w.Envelope),
		"envelope embutido preserva bytes do Kafka record")
}

func TestProcessRecordDedupsFanoutOnSecondCall(t *testing.T) {
	pool, teardown := startPostgres(t)
	defer teardown()

	reg := stubRegistry{
		"order.created": {
			{ID: "a", CallbackURL: "http://a/dispatch"},
			{ID: "b", CallbackURL: "http://b/dispatch"},
		},
	}
	c := newTestConsumer(pool, reg)

	env := wireEnvelope{
		EventID: uuid.New(), EventType: "order.created",
		CorrelationID: "saga-3", Payload: json.RawMessage(`{}`),
		OccurredAt: time.Now().UTC(),
	}
	rec := newRecord("order.created", env)

	ctx := context.Background()
	for range 2 {
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
		require.NoError(t, err)
		require.NoError(t, c.processRecord(ctx, tx, rec))
		require.NoError(t, tx.Commit(ctx))
	}

	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM dispatch_outbox WHERE event_id=$1`, env.EventID).Scan(&n))
	assert.Equal(t, 2, n, "ON CONFLICT DO NOTHING garante N linhas (não 2N)")
}

func TestProcessRecordSkipsUnsubscribedType(t *testing.T) {
	pool, teardown := startPostgres(t)
	defer teardown()

	reg := stubRegistry{} // vazio: nenhum event_type registrado
	c := newTestConsumer(pool, reg)

	env := wireEnvelope{
		EventID: uuid.New(), EventType: "unknown.event",
		CorrelationID: "saga-4", Payload: json.RawMessage(`{}`),
	}
	rec := newRecord("unknown.event", env)

	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, c.processRecord(ctx, tx, rec))
	require.NoError(t, tx.Commit(ctx))

	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM dispatch_outbox`).Scan(&n))
	assert.Equal(t, 0, n, "sem subscribers → nenhuma linha inserida")
}

func TestProcessRecordSkipsMalformedPayload(t *testing.T) {
	pool, teardown := startPostgres(t)
	defer teardown()

	reg := stubRegistry{
		"order.created": {{ID: "x", CallbackURL: "http://x/dispatch"}},
	}
	c := newTestConsumer(pool, reg)

	rec := &kgo.Record{Topic: "order.created", Value: []byte("not json at all")}

	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, c.processRecord(ctx, tx, rec),
		"payload malformado NÃO deve travar o consumer (só loga + métrica)")
	require.NoError(t, tx.Commit(ctx))

	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM dispatch_outbox`).Scan(&n))
	assert.Equal(t, 0, n, "malformed → skip; nenhuma linha inserida")
}
