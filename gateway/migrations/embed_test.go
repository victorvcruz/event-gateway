package migrations_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/victorvcruz/event-gateway/gateway/migrations"
	sharedmig "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
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

	return dsn, func() { _ = container.Terminate(context.Background()) }
}

func applyAllUp(t *testing.T, dsn string) {
	t.Helper()
	require.NoError(t, sharedmig.Apply(dsn))
	require.NoError(t, sharedmig.Apply(dsn, sharedmig.WithTableName("dispatch_outbox")))

	m := newMigrate(t, dsn)
	defer func() { _, _ = m.Close() }()
	require.NoError(t, m.Up())
}

func countTables(t *testing.T, pool *pgxpool.Pool, names ...string) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM information_schema.tables
		 WHERE table_schema='public' AND table_name = ANY($1)`,
		names,
	).Scan(&n))
	return n
}

func newMigrate(t *testing.T, dsn string) *migrate.Migrate {
	t.Helper()
	src, err := iofs.New(migrations.FS, ".")
	require.NoError(t, err)
	m, err := migrate.NewWithSourceInstance("iofs", src, dsn)
	require.NoError(t, err)
	return m
}

func TestUpDownUpCycleAllLayers(t *testing.T) {
	dsn, teardown := startPostgres(t)
	defer teardown()

	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	defer pool.Close()

	// up #1 — todas as camadas
	applyAllUp(t, dsn)
	assert.Equal(t, 4, countTables(t, pool,
		"outbox", "dispatch_outbox", "event_log", "dead_letter"))

	// down (ordem inversa: gateway → base do dispatch_outbox → base do outbox)
	m := newMigrate(t, dsn)
	require.NoError(t, m.Down())
	_, _ = m.Close()
	require.NoError(t, sharedmig.Down(dsn, sharedmig.WithTableName("dispatch_outbox")))
	require.NoError(t, sharedmig.Down(dsn))
	assert.Equal(t, 0, countTables(t, pool,
		"outbox", "dispatch_outbox", "event_log", "dead_letter"))

	// up #2 — todas as camadas de novo
	applyAllUp(t, dsn)
	assert.Equal(t, 4, countTables(t, pool,
		"outbox", "dispatch_outbox", "event_log", "dead_letter"))
}

func TestSpecializeMakesCompositePK(t *testing.T) {
	dsn, teardown := startPostgres(t)
	defer teardown()

	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	defer pool.Close()

	applyAllUp(t, dsn)

	rows, err := pool.Query(context.Background(), `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
		WHERE c.relname='dispatch_outbox' AND i.indisprimary
		ORDER BY a.attnum`)
	require.NoError(t, err)
	defer rows.Close()

	var pk []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		pk = append(pk, name)
	}
	assert.Equal(t, []string{"event_id", "service_id"}, pk,
		"specialize migration must recreate PK as (event_id, service_id)")
}

func TestUpIsIdempotent(t *testing.T) {
	dsn, teardown := startPostgres(t)
	defer teardown()

	applyAllUp(t, dsn)

	// reaplica as migrations do gateway — não deve fazer nada
	m := newMigrate(t, dsn)
	err := m.Up()
	assert.ErrorIs(t, err, migrate.ErrNoChange)
	_, _ = m.Close()

	// reaplica a base de shared/outbox — não deve fazer nada
	require.NoError(t, sharedmig.Apply(dsn))
	require.NoError(t, sharedmig.Apply(dsn, sharedmig.WithTableName("dispatch_outbox")))
}
