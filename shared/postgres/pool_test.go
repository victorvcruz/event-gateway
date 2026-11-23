package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/jackc/pgx/v5"

	"github.com/victorvcruz/event-gateway/shared/postgres"
)

func startPostgres(t *testing.T) (dsn string, teardown func()) {
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

	dsn, err = container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	return dsn, func() {
		_ = container.Terminate(context.Background())
	}
}

func TestNewPoolPing(t *testing.T) {
	dsn, teardown := startPostgres(t)
	defer teardown()

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	assert.NoError(t, postgres.Ping(ctx, pool))
}

func TestWithTxCommit(t *testing.T) {
	dsn, teardown := startPostgres(t)
	defer teardown()

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(ctx, "CREATE TABLE t (n INT)")
	require.NoError(t, err)

	err = postgres.WithTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
		_, e := tx.Exec(ctx, "INSERT INTO t (n) VALUES (1), (2)")
		return e
	})
	require.NoError(t, err)

	var n int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM t").Scan(&n))
	assert.Equal(t, 2, n)
}

func TestWithTxRollback(t *testing.T) {
	dsn, teardown := startPostgres(t)
	defer teardown()

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(ctx, "CREATE TABLE t (n INT)")
	require.NoError(t, err)

	sentinel := errors.New("boom")
	err = postgres.WithTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, "INSERT INTO t (n) VALUES (1)"); e != nil {
			return e
		}
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)

	var n int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM t").Scan(&n))
	assert.Equal(t, 0, n)
}
