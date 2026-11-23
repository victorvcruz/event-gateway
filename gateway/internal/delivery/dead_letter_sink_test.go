package delivery_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/victorvcruz/event-gateway/gateway/internal/delivery"
	"github.com/victorvcruz/event-gateway/gateway/internal/repository"
	"github.com/victorvcruz/event-gateway/gateway/migrations"
	"github.com/victorvcruz/event-gateway/shared/outbox"
	outboxmig "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
)

func startMigrated(t *testing.T) (*pgxpool.Pool, func()) {
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
	require.NoError(t, outboxmig.Apply(dsn))
	require.NoError(t, outboxmig.Apply(dsn, outboxmig.WithTableName("dispatch_outbox")))
	require.NoError(t, migrations.Apply(dsn))

	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)

	return pool, func() {
		pool.Close()
		_ = container.Terminate(context.Background())
	}
}

func seedOutbox(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO outbox (event_id, event_type, payload, correlation_id, status, retry_count)
		VALUES ($1, 'order.created', $2, 'saga-x', 'PENDING', 4)
	`, id, json.RawMessage(`{"n":1}`))
	require.NoError(t, err)
}

func TestDeadLetterSinkAtomicUpdateInsert(t *testing.T) {
	pool, teardown := startMigrated(t)
	defer teardown()

	client := outbox.NewClient(pool, outbox.DefaultTable)
	repo := repository.NewDeadLetter(pool)
	metric := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "dead_letter_total"},
		[]string{"event_type"},
	)
	sink := delivery.NewDeadLetterSink(client, repo, metric, "")

	id := uuid.New()
	seedOutbox(t, pool, id)

	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)

	row := outbox.Row{
		EventID:       id,
		EventType:     "order.created",
		CorrelationID: "saga-x",
		Payload:       json.RawMessage(`{"n":1}`),
		RetryCount:    5,
	}
	require.NoError(t, sink.Handle(ctx, tx, row, errors.New("kafka: broker down")))
	require.NoError(t, tx.Commit(ctx))

	// outbox.status = DEAD, retry_count = 5
	var status, lastError string
	var retry int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status, retry_count, last_error FROM outbox WHERE event_id=$1`, id,
	).Scan(&status, &retry, &lastError))
	assert.Equal(t, "DEAD", status)
	assert.Equal(t, 5, retry)
	assert.Equal(t, "kafka: broker down", lastError)

	// dead_letter row inserida com service_id e failure_reason.
	var serviceID, reason, eventType string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT service_id, failure_reason, event_type FROM dead_letter WHERE event_id=$1`, id,
	).Scan(&serviceID, &reason, &eventType))
	assert.Equal(t, delivery.ServiceIDKafkaBroker, serviceID)
	assert.Equal(t, "kafka: broker down", reason)
	assert.Equal(t, "order.created", eventType)

	// Métrica incrementada com o label event_type.
	assert.InDelta(t, 1.0, testutil.ToFloat64(metric.WithLabelValues("order.created")), 0.0001)
}

func TestDeadLetterSinkRollbackKeepsBothClean(t *testing.T) {
	pool, teardown := startMigrated(t)
	defer teardown()

	client := outbox.NewClient(pool, outbox.DefaultTable)
	repo := repository.NewDeadLetter(pool)
	metric := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "dead_letter_total_rb"},
		[]string{"event_type"},
	)
	sink := delivery.NewDeadLetterSink(client, repo, metric, "")

	id := uuid.New()
	seedOutbox(t, pool, id)

	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, sink.Handle(ctx, tx, outbox.Row{
		EventID: id, EventType: "order.created", Payload: json.RawMessage(`{}`),
		RetryCount: 5,
	}, errors.New("boom")))
	require.NoError(t, tx.Rollback(ctx))

	// Nem outbox nem dead_letter mudam após rollback.
	var status string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status FROM outbox WHERE event_id=$1`, id,
	).Scan(&status))
	assert.Equal(t, "PENDING", status, "rollback preserva PENDING")

	var deadCount int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM dead_letter WHERE event_id=$1`, id,
	).Scan(&deadCount))
	assert.Equal(t, 0, deadCount, "rollback preserva dead_letter vazia")
}

// seedDispatchOutbox insere uma linha em dispatch_outbox com payload
// contendo o wrapper DispatchEnvelope.
func seedDispatchOutbox(t *testing.T, pool *pgxpool.Pool,
	eventID uuid.UUID, serviceID, callbackURL string,
) outbox.Row {
	t.Helper()
	env := json.RawMessage(`{"event_id":"x","payload":{"amount":9900}}`)
	wrapper, err := delivery.EncodeDispatchEnvelope(
		delivery.TargetMeta{ServiceID: serviceID, CallbackURL: callbackURL}, env,
	)
	require.NoError(t, err)

	_, err = pool.Exec(context.Background(), `
		INSERT INTO dispatch_outbox
		  (event_id, service_id, callback_url, event_type, correlation_id,
		   payload, occurred_at, status, retry_count, next_attempt_at)
		VALUES ($1, $2, $3, 'order.created', 'saga-1', $4, now(), 'PENDING', 0, now())`,
		eventID, serviceID, callbackURL, wrapper,
	)
	require.NoError(t, err)
	return outbox.Row{
		EventID:       eventID,
		EventType:     "order.created",
		CorrelationID: "saga-1",
		Payload:       wrapper,
		RetryCount:    3,
	}
}

func TestDeadLetterSinkDispatchModeUsesWrapperServiceID(t *testing.T) {
	pool, teardown := startMigrated(t)
	defer teardown()

	client := outbox.NewClient(pool, "dispatch_outbox")
	dispatch := repository.NewDispatch(pool)
	deadLetterRepo := repository.NewDeadLetter(pool)
	relayMetric := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "test_relay_dead_letter_total"},
		[]string{"event_type"},
	)
	dispatchMetric := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "test_dispatch_dead_letter_total"},
		[]string{"event_type", "service_id"},
	)
	sink := delivery.NewDeadLetterSink(client, deadLetterRepo, relayMetric,
		delivery.ServiceIDKafkaBroker,
	).WithDispatch(dispatch, dispatchMetric)

	eventID := uuid.New()
	row := seedDispatchOutbox(t, pool, eventID, "payment-service", "http://payment/dispatch")

	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, sink.Handle(ctx, tx, row, errors.New("http 500 boom")))
	require.NoError(t, tx.Commit(ctx))

	// dispatch_outbox → DEAD por (event_id, service_id)
	var status, lastError string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT status, last_error FROM dispatch_outbox
		WHERE event_id=$1 AND service_id=$2`, eventID, "payment-service").Scan(&status, &lastError))
	assert.Equal(t, "DEAD", status)
	assert.Equal(t, "http 500 boom", lastError)

	// dead_letter.service_id vem do WRAPPER, NÃO do serviceID do construtor
	var svc string
	var payload []byte
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT service_id, payload FROM dead_letter
		WHERE event_id=$1`, eventID).Scan(&svc, &payload))
	assert.Equal(t, "payment-service", svc,
		"NÃO deve ser 'kafka-broker' — modo Dispatch usa wrapper.Target.ServiceID")

	// payload gravado em dead_letter é o envelope RAW (env.Envelope), não o wrapper
	assert.JSONEq(t, `{"event_id":"x","payload":{"amount":9900}}`, string(payload),
		"dead_letter.payload = env.Envelope; wrapper NUNCA vaza como evidência")

	// métrica dispatch (não a de Kafka relay)
	assert.InDelta(t, 1.0,
		testutil.ToFloat64(dispatchMetric.WithLabelValues("order.created", "payment-service")),
		0.001)
	assert.InDelta(t, 0.0,
		testutil.ToFloat64(relayMetric.WithLabelValues("order.created")),
		0.001, "métrica de Kafka relay NÃO deve mexer no modo Dispatch")
}

func TestDeadLetterSinkDispatchModeRollbackKeepsBothClean(t *testing.T) {
	pool, teardown := startMigrated(t)
	defer teardown()

	client := outbox.NewClient(pool, "dispatch_outbox")
	dispatch := repository.NewDispatch(pool)
	deadLetterRepo := repository.NewDeadLetter(pool)
	sink := delivery.NewDeadLetterSink(client, deadLetterRepo,
		prometheus.NewCounterVec(prometheus.CounterOpts{Name: "unused"}, []string{"event_type"}),
		delivery.ServiceIDKafkaBroker,
	).WithDispatch(dispatch,
		prometheus.NewCounterVec(prometheus.CounterOpts{Name: "unused2"}, []string{"event_type", "service_id"}))

	eventID := uuid.New()
	row := seedDispatchOutbox(t, pool, eventID, "inventory-service", "http://inv/dispatch")

	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, sink.Handle(ctx, tx, row, errors.New("boom")))
	require.NoError(t, tx.Rollback(ctx))

	// dispatch_outbox permanece PENDING
	var status string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT status FROM dispatch_outbox
		WHERE event_id=$1 AND service_id=$2`, eventID, "inventory-service").Scan(&status))
	assert.Equal(t, "PENDING", status)

	// dead_letter vazio
	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM dead_letter WHERE event_id=$1`, eventID).Scan(&n))
	assert.Equal(t, 0, n)
}

func TestDeadLetterSinkRelayModeUnchangedWithDispatchEnabled(t *testing.T) {
	// WithDispatch não pode mudar o caminho do relay: quando o payload não é
	// um wrapper, o sink usa o outbox client e o serviceID do construtor.
	pool, teardown := startMigrated(t)
	defer teardown()

	client := outbox.NewClient(pool, outbox.DefaultTable)
	dispatch := repository.NewDispatch(pool)
	deadLetterRepo := repository.NewDeadLetter(pool)
	relayMetric := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "test_relay_dead_letter_total2"},
		[]string{"event_type"},
	)
	dispatchMetric := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "test_dispatch_dead_letter_total2"},
		[]string{"event_type", "service_id"},
	)
	sink := delivery.NewDeadLetterSink(client, deadLetterRepo, relayMetric,
		delivery.ServiceIDKafkaBroker,
	).WithDispatch(dispatch, dispatchMetric)

	// seed em outbox: o payload é o envelope cru, sem wrapper
	id := uuid.New()
	rawPayload := json.RawMessage(`{"order_id":"abc"}`)
	_, err := pool.Exec(context.Background(), `
		INSERT INTO outbox (event_id, event_type, payload, correlation_id, status)
		VALUES ($1, 'order.created', $2, 'saga-x', 'PENDING')`, id, rawPayload)
	require.NoError(t, err)

	row := outbox.Row{
		EventID:       id,
		EventType:     "order.created",
		CorrelationID: "saga-x",
		Payload:       rawPayload,
		RetryCount:    5,
	}

	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, sink.Handle(ctx, tx, row, errors.New("kafka broker down")))
	require.NoError(t, tx.Commit(ctx))

	// outbox → DEAD (não dispatch_outbox)
	var status string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status FROM outbox WHERE event_id=$1`, id).Scan(&status))
	assert.Equal(t, "DEAD", status)

	// dead_letter.service_id = "kafka-broker" (fallback ao construtor)
	var svc string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT service_id FROM dead_letter WHERE event_id=$1`, id).Scan(&svc))
	assert.Equal(t, delivery.ServiceIDKafkaBroker, svc)

	// métricas: relay ativa, dispatch NÃO
	assert.InDelta(t, 1.0,
		testutil.ToFloat64(relayMetric.WithLabelValues("order.created")), 0.001)
	assert.InDelta(t, 0.0,
		testutil.ToFloat64(dispatchMetric.WithLabelValues("order.created", "kafka-broker")), 0.001)
}
