package migrations_test

import (
	"context"
	_ "embed"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/victorvcruz/event-gateway/shared/outbox/migrations"
)

//go:embed testdata/outbox_default_schema.sql
var baselineDefaultSchema string

func startPostgres(t *testing.T) (string, *pgxpool.Pool, func()) {
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

	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)

	return dsn, pool, func() {
		pool.Close()
		_ = container.Terminate(context.Background())
	}
}

func dumpTableSchema(t *testing.T, pool *pgxpool.Pool, table string) string {
	t.Helper()
	ctx := context.Background()
	var out strings.Builder

	rows, err := pool.Query(ctx, `
		SELECT column_name, data_type, character_maximum_length,
		       is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema='public' AND table_name=$1
		ORDER BY ordinal_position`, table)
	require.NoError(t, err)
	for rows.Next() {
		var name, dtype, nullable string
		var maxLen *int
		var deflt *string
		require.NoError(t, rows.Scan(&name, &dtype, &maxLen, &nullable, &deflt))
		out.WriteString("col " + name + " " + dtype)
		if maxLen != nil {
			out.WriteString("(" + intToString(*maxLen) + ")")
		}
		if nullable == "NO" {
			out.WriteString(" NOT NULL")
		}
		if deflt != nil {
			out.WriteString(" DEFAULT " + *deflt)
		}
		out.WriteString("\n")
	}
	rows.Close()

	idxRows, err := pool.Query(ctx, `
		SELECT indexname, indexdef
		FROM pg_indexes
		WHERE schemaname='public' AND tablename=$1
		ORDER BY indexname`, table)
	require.NoError(t, err)
	for idxRows.Next() {
		var name, def string
		require.NoError(t, idxRows.Scan(&name, &def))
		out.WriteString("idx " + name + " " + def + "\n")
	}
	idxRows.Close()
	return out.String()
}

func intToString(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := make([]byte, 0, 12)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}

func TestApplyDefaultBackwardCompatible(t *testing.T) {
	dsn, pool, teardown := startPostgres(t)
	defer teardown()

	require.NoError(t, migrations.Apply(dsn))

	current := dumpTableSchema(t, pool, "outbox")
	baseline := canonicalize(baselineDefaultSchema)

	assert.Contains(t, baseline, "CREATE TABLE public.outbox")
	assert.Contains(t, current, "col event_id uuid NOT NULL")
	assert.Contains(t, current, "col event_type character varying(255) NOT NULL")
	assert.Contains(t, current, "col payload jsonb NOT NULL")
	assert.Contains(t, current, "col correlation_id character varying(255)")
	assert.Contains(t, current, "col status character varying(20) NOT NULL DEFAULT 'PENDING'")
	assert.Contains(t, current, "col retry_count integer NOT NULL DEFAULT 0")
	assert.Contains(t, current, "col created_at timestamp with time zone NOT NULL DEFAULT now()")
	assert.Contains(t, current, "col sent_at timestamp with time zone")
	assert.Contains(t, current, "idx outbox_pkey")
	assert.Contains(t, current, "idx idx_outbox_pending")
	assert.Contains(t, current, "WHERE ((status)::text = 'PENDING'::text)")
}

var wsRe = regexp.MustCompile(`\s+`)

func canonicalize(s string) string {
	return wsRe.ReplaceAllString(s, " ")
}

func TestApplyWithTableNameCreatesIndependentTable(t *testing.T) {
	dsn, pool, teardown := startPostgres(t)
	defer teardown()

	require.NoError(t, migrations.Apply(dsn))
	require.NoError(t, migrations.Apply(dsn, migrations.WithTableName("dispatch_outbox")))

	both := dumpTableSchema(t, pool, "outbox") + dumpTableSchema(t, pool, "dispatch_outbox")
	assert.Contains(t, both, "col event_id uuid NOT NULL")
	assert.Contains(t, both, "idx idx_outbox_pending")
	assert.Contains(t, both, "idx idx_dispatch_outbox_pending")

	countA := countRows(t, pool, "schema_migrations_outbox")
	countB := countRows(t, pool, "schema_migrations_dispatch_outbox")
	assert.Positive(t, countA)
	assert.Positive(t, countB)
}

func countRows(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM "+table).Scan(&n)
	require.NoError(t, err)
	return n
}

func TestApplyIdempotent(t *testing.T) {
	dsn, _, teardown := startPostgres(t)
	defer teardown()

	require.NoError(t, migrations.Apply(dsn))
	require.NoError(t, migrations.Apply(dsn))
	require.NoError(t, migrations.Apply(dsn, migrations.WithTableName("dispatch_outbox")))
	require.NoError(t, migrations.Apply(dsn, migrations.WithTableName("dispatch_outbox")))
}

func TestApplyRejectsInvalidTableName(t *testing.T) {
	dsn, pool, teardown := startPostgres(t)
	defer teardown()

	err := migrations.Apply(dsn, migrations.WithTableName("bad; DROP TABLE users;"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, migrations.ErrInvalidIdentifier))

	err = migrations.Apply(dsn, migrations.WithTableName(""))
	require.Error(t, err)

	err = migrations.Apply(dsn, migrations.WithTableName("with space"))
	require.Error(t, err)

	var count int
	err = pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM information_schema.tables
		 WHERE table_schema='public' AND table_name LIKE 'bad%'`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestUpDownUpCycle(t *testing.T) {
	dsn, pool, teardown := startPostgres(t)
	defer teardown()

	for _, name := range []string{"outbox", "dispatch_outbox"} {
		opts := []migrations.Option{}
		if name != "outbox" {
			opts = append(opts, migrations.WithTableName(name))
		}

		require.NoError(t, migrations.Apply(dsn, opts...), name+" up#1")
		require.NoError(t, migrations.Down(dsn, opts...), name+" down")

		var exists bool
		err := pool.QueryRow(context.Background(),
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables
			                WHERE table_schema='public' AND table_name=$1)`,
			name).Scan(&exists)
		require.NoError(t, err)
		assert.False(t, exists, name+" should be gone after Down")

		require.NoError(t, migrations.Apply(dsn, opts...), name+" up#2")
		err = pool.QueryRow(context.Background(),
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables
			                WHERE table_schema='public' AND table_name=$1)`,
			name).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, name+" should exist after up#2")
	}
}
