package delivery_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/victorvcruz/event-gateway/gateway/internal/delivery"
	"github.com/victorvcruz/event-gateway/gateway/internal/repository"
	gwmig "github.com/victorvcruz/event-gateway/gateway/migrations"
	"github.com/victorvcruz/event-gateway/shared/outbox"
	sharedmig "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
)

func startHookPool(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pg, err := tcpostgres.Run(ctx,
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

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	require.NoError(t, sharedmig.Apply(dsn))
	require.NoError(t, sharedmig.Apply(dsn, sharedmig.WithTableName("dispatch_outbox")))
	require.NoError(t, gwmig.Apply(dsn))

	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	return pool, func() {
		pool.Close()
		_ = pg.Terminate(context.Background())
	}
}

func seedDispatchPending(t *testing.T, pool *pgxpool.Pool, eventID uuid.UUID, serviceID string) outbox.Row {
	t.Helper()
	wrapper, err := delivery.EncodeDispatchEnvelope(
		delivery.TargetMeta{ServiceID: serviceID, CallbackURL: "http://" + serviceID + "/dispatch"},
		json.RawMessage(`{"order_id":"x"}`),
	)
	require.NoError(t, err)

	_, err = pool.Exec(context.Background(), `
		INSERT INTO dispatch_outbox
		  (event_id, service_id, callback_url, event_type, correlation_id,
		   payload, occurred_at, status, retry_count, next_attempt_at)
		VALUES ($1, $2, $3, 'order.created', 'saga-1', $4, now(), 'PENDING', 0, now())`,
		eventID, serviceID, "http://"+serviceID+"/dispatch", wrapper,
	)
	require.NoError(t, err)
	return outbox.Row{
		EventID:       eventID,
		EventType:     "order.created",
		CorrelationID: "saga-1",
		Payload:       wrapper,
	}
}

func TestSuccessHookGravaSentEDispatchedNaMesmaTx(t *testing.T) {
	pool, teardown := startHookPool(t)
	defer teardown()

	eventID := uuid.New()
	row := seedDispatchPending(t, pool, eventID, "payment-service")

	dispatch := repository.NewDispatch(pool)
	latency := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "test_dispatch_latency_seconds"},
		[]string{"event_type", "service_id"},
	)
	hook := delivery.NewSuccessHookEventLog(dispatch, latency)

	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, hook.Handle(ctx, tx, row))
	require.NoError(t, tx.Commit(ctx))

	var status string
	var sentAt *time.Time
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT status, sent_at FROM dispatch_outbox
		WHERE event_id=$1 AND service_id=$2`, eventID, "payment-service",
	).Scan(&status, &sentAt))
	assert.Equal(t, "SENT", status)
	require.NotNil(t, sentAt)

	var dir, svc string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT direction, service_id FROM event_log
		WHERE event_id=$1 AND direction='DISPATCHED'`, eventID,
	).Scan(&dir, &svc))
	assert.Equal(t, "DISPATCHED", dir)
	assert.Equal(t, "payment-service", svc)
}

func TestSuccessHookRollbackKeepsPending(t *testing.T) {
	pool, teardown := startHookPool(t)
	defer teardown()

	eventID := uuid.New()
	row := seedDispatchPending(t, pool, eventID, "inventory-service")

	dispatch := repository.NewDispatch(pool)
	hook := delivery.NewSuccessHookEventLog(dispatch, nil)

	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, hook.Handle(ctx, tx, row))
	require.NoError(t, tx.Rollback(ctx))

	var status string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status FROM dispatch_outbox WHERE event_id=$1`, eventID).Scan(&status))
	assert.Equal(t, "PENDING", status, "rollback deve reverter status")

	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM event_log WHERE event_id=$1`, eventID).Scan(&n))
	assert.Equal(t, 0, n, "rollback deve reverter event_log INSERT")
}

func TestSuccessHookRejectsCorruptedWrapper(t *testing.T) {
	pool, teardown := startHookPool(t)
	defer teardown()

	dispatch := repository.NewDispatch(pool)
	hook := delivery.NewSuccessHookEventLog(dispatch, nil)

	badRow := outbox.Row{
		EventID:       uuid.New(),
		EventType:     "order.created",
		CorrelationID: "saga",
		Payload:       json.RawMessage("not a wrapper"),
	}

	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()

	err = hook.Handle(ctx, tx, badRow)
	require.Error(t, err, "wrapper corrompido deve abortar (Worker faz rollback)")
}
