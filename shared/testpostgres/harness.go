// Package testpostgres sobe um PostgreSQL isolado para os testes de
// integração. Só deve ser importado por arquivos _test.go.
package testpostgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	servicepostgres "github.com/victorvcruz/event-gateway/shared/postgres"
)

type Harness struct {
	DSN  string
	Pool *pgxpool.Pool
}

func Start(t *testing.T) *Harness {
	t.Helper()
	if testing.Short() {
		t.Skip("PostgreSQL integration test skipped in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("service_test"),
		tcpostgres.WithUsername("service"),
		tcpostgres.WithPassword("service"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = container.Terminate(context.Background())
		t.Fatalf("postgres connection string: %v", err)
	}
	pool, err := servicepostgres.NewPool(ctx, dsn)
	if err != nil {
		_ = container.Terminate(context.Background())
		t.Fatalf("open postgres pool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		terminateCtx, terminateCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer terminateCancel()
		if err := container.Terminate(terminateCtx); err != nil {
			t.Errorf("terminate postgres container: %v", err)
		}
	})
	return &Harness{DSN: dsn, Pool: pool}
}

func (h *Harness) Apply(t *testing.T, migrations ...func(string) error) {
	t.Helper()
	for _, apply := range migrations {
		if err := apply(h.DSN); err != nil {
			t.Fatalf("apply migrations: %v", err)
		}
	}
}
