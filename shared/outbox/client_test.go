package outbox_test

import (
	"context"
	"encoding/json"
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

	"github.com/victorvcruz/event-gateway/shared/outbox"
	outboxmig "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
)

func startPoolMigrated(t *testing.T) (*pgxpool.Pool, string, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("app_db"),
		tcpostgres.WithUsername("app"),
		tcpostgres.WithPassword("app"),
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

	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)

	return pool, dsn, func() {
		pool.Close()
		_ = container.Terminate(context.Background())
	}
}

func inTx(t *testing.T, pool *pgxpool.Pool, fn func(tx pgx.Tx)) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	fn(tx)
	require.NoError(t, tx.Commit(ctx))
}

func TestApplyCreatesOutboxAndTrackingTable(t *testing.T) {
	_, dsn, teardown := startPoolMigrated(t)
	defer teardown()

	// O segundo Apply é idempotente.
	require.NoError(t, outboxmig.Apply(dsn))

	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	defer pool.Close()

	var count int
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT count(*) FROM information_schema.tables
		 WHERE table_schema='public'
		   AND table_name IN ('outbox', 'schema_migrations_outbox')
	`).Scan(&count))
	assert.Equal(t, 2, count, "outbox + schema_migrations_outbox devem existir")
}

func TestClientInsertAndDuplicate(t *testing.T) {
	pool, _, teardown := startPoolMigrated(t)
	defer teardown()

	c := outbox.NewClient(pool, "")
	assert.Equal(t, outbox.DefaultTable, c.Table())

	row := outbox.Row{
		EventID:       uuid.New(),
		EventType:     "order.created",
		CorrelationID: "saga-42",
		Payload:       json.RawMessage(`{"k":"v"}`),
	}
	inTx(t, pool, func(tx pgx.Tx) {
		require.NoError(t, c.InsertPending(context.Background(), tx, row))
	})

	ctx := context.Background()
	tx2, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	err = c.InsertPending(ctx, tx2, row)
	assert.ErrorIs(t, err, outbox.ErrDuplicate)
	_ = tx2.Rollback(ctx)
}

func TestClientLookupStatusAndMarkSent(t *testing.T) {
	pool, _, teardown := startPoolMigrated(t)
	defer teardown()

	c := outbox.NewClient(pool, "")
	id := uuid.New()

	inTx(t, pool, func(tx pgx.Tx) {
		require.NoError(t, c.InsertPending(context.Background(), tx, outbox.Row{
			EventID: id, EventType: "order.created", CorrelationID: "c",
			Payload: json.RawMessage(`{}`),
		}))
	})

	inTx(t, pool, func(tx pgx.Tx) {
		s, err := c.LookupStatus(context.Background(), tx, id)
		require.NoError(t, err)
		assert.Equal(t, "PENDING", s)
	})

	// LookupStatusPool não depende de tx externa.
	s, err := c.LookupStatusPool(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, "PENDING", s)

	inTx(t, pool, func(tx pgx.Tx) {
		require.NoError(t, c.MarkSent(context.Background(), tx, id, time.Now()))
	})

	inTx(t, pool, func(tx pgx.Tx) {
		s, err := c.LookupStatus(context.Background(), tx, id)
		require.NoError(t, err)
		assert.Equal(t, "SENT", s)
	})
}

func TestClientSelectPendingSkipsLocked(t *testing.T) {
	pool, _, teardown := startPoolMigrated(t)
	defer teardown()

	c := outbox.NewClient(pool, "")
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}

	inTx(t, pool, func(tx pgx.Tx) {
		for _, id := range ids {
			require.NoError(t, c.InsertPending(context.Background(), tx, outbox.Row{
				EventID: id, EventType: "order.created", CorrelationID: "c",
				Payload: json.RawMessage(`{}`),
			}))
		}
	})

	ctx := context.Background()

	txA, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	defer txA.Rollback(ctx)

	rowsA, err := c.SelectPending(ctx, txA, 10)
	require.NoError(t, err)
	assert.Len(t, rowsA, 3)

	txB, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	defer txB.Rollback(ctx)

	rowsB, err := c.SelectPending(ctx, txB, 10)
	require.NoError(t, err)
	assert.Empty(t, rowsB, "FOR UPDATE SKIP LOCKED")
}

func TestClientMarkDead(t *testing.T) {
	pool, _, teardown := startPoolMigrated(t)
	defer teardown()

	c := outbox.NewClient(pool, "")
	id := uuid.New()

	inTx(t, pool, func(tx pgx.Tx) {
		require.NoError(t, c.InsertPending(context.Background(), tx, outbox.Row{
			EventID: id, EventType: "order.created", CorrelationID: "c",
			Payload: json.RawMessage(`{}`),
		}))
	})

	inTx(t, pool, func(tx pgx.Tx) {
		require.NoError(t, c.MarkDead(context.Background(), tx, id, 5))
	})

	inTx(t, pool, func(tx pgx.Tx) {
		s, err := c.LookupStatus(context.Background(), tx, id)
		require.NoError(t, err)
		assert.Equal(t, "DEAD", s)
	})
}
