package repository_test

import (
	"context"
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

	"github.com/victorvcruz/event-gateway/gateway/internal/repository"
	"github.com/victorvcruz/event-gateway/gateway/migrations"
	outboxmig "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
)

func startAndMigrate(t *testing.T) (*pgxpool.Pool, func()) {
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

func inTx(t *testing.T, pool *pgxpool.Pool, fn func(tx pgx.Tx)) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	fn(tx)
	require.NoError(t, tx.Commit(ctx))
}

func TestEventLogAppendReceived(t *testing.T) {
	pool, teardown := startAndMigrate(t)
	defer teardown()

	logRepo := repository.NewEventLog(pool)
	id := uuid.New()

	inTx(t, pool, func(tx pgx.Tx) {
		require.NoError(t, logRepo.AppendReceived(context.Background(), tx, id, "saga-1", "order.created"))
	})

	var direction, eventType string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT direction, event_type FROM event_log WHERE event_id=$1`, id,
	).Scan(&direction, &eventType))
	assert.Equal(t, "RECEIVED", direction)
	assert.Equal(t, "order.created", eventType)
}
