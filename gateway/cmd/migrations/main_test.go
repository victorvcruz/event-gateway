package main_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const configEnv = "EVENT_GATEWAY_CONFIG"

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

func writeConfig(t *testing.T, dsn string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := fmt.Sprintf(`
database:
  dsn: %q
kafka:
  brokers: [kafka:9092]
schema_registry:
  endpoint: http://schema-registry:8081
`, dsn)
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func TestMainReadsConfigAndAppliesUp(t *testing.T) {
	dsn, teardown := startPostgres(t)
	defer teardown()

	cfgPath := writeConfig(t, dsn)

	cmd := exec.Command("go", "run", ".")
	cmd.Env = append(os.Environ(), configEnv+"="+cfgPath)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	defer pool.Close()

	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM information_schema.tables
		 WHERE table_schema='public' AND table_name IN ('outbox','event_log','dead_letter')`,
	).Scan(&n))
	assert.Equal(t, 3, n, "expected 3 tables after up (outbox from shared, event_log + dead_letter from gateway)")
}

func TestMainIsIdempotent(t *testing.T) {
	dsn, teardown := startPostgres(t)
	defer teardown()

	cfgPath := writeConfig(t, dsn)

	for range 2 {
		cmd := exec.Command("go", "run", ".")
		cmd.Env = append(os.Environ(), configEnv+"="+cfgPath)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
	}
}
