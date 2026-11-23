package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/victorvcruz/event-gateway/gateway/internal/ingest"
	"github.com/victorvcruz/event-gateway/gateway/internal/observability"
	"github.com/victorvcruz/event-gateway/gateway/internal/repository"
	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
	transporthttp "github.com/victorvcruz/event-gateway/gateway/internal/transport/http"
	"github.com/victorvcruz/event-gateway/gateway/migrations"
	"github.com/victorvcruz/event-gateway/gateway/pkg/client"
	"github.com/victorvcruz/event-gateway/shared/outbox"
	outboxmig "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
)

type sdkEnv struct {
	pool      *pgxpool.Pool
	serverURL string
	close     func()
}

func newSDKEnv(t *testing.T) *sdkEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pgContainer, err := tcpostgres.Run(ctx,
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

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	require.NoError(t, outboxmig.Apply(dsn))
	require.NoError(t, outboxmig.Apply(dsn, outboxmig.WithTableName("dispatch_outbox")))
	require.NoError(t, migrations.Apply(dsn))

	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)

	loader := schema.NewLoader()
	require.NoError(t, loader.Load(context.Background()))

	metrics := observability.New()
	outboxClient := outbox.NewClient(pool, outbox.DefaultTable)
	eventLogRepo := repository.NewEventLog(pool)
	svc := ingest.NewService(pool, outboxClient, eventLogRepo, loader)

	deps := transporthttp.Deps{
		Ingest:         svc,
		Registry:       metrics.Registry,
		IngestLatency:  metrics.IngestLatency,
		EventsReceived: metrics.EventsReceived,
	}
	server := httptest.NewServer(transporthttp.NewRouter(deps))

	return &sdkEnv{
		pool:      pool,
		serverURL: server.URL,
		close: func() {
			server.Close()
			pool.Close()
			_ = pgContainer.Terminate(context.Background())
		},
	}
}

// TestClientPublishAccepted: o SDK publica um evento válido e recebe 202.
func TestClientPublishHappyPath(t *testing.T) {
	env := newSDKEnv(t)
	defer env.close()

	cli, err := client.New(env.serverURL)
	require.NoError(t, err)

	evt := client.Event{
		EventID:       uuid.New(),
		EventType:     "order.created",
		CorrelationID: "sdk-t12",
		Payload: json.RawMessage(`{
			"order_id":"6c8a1d7f-6e3f-4f13-9b9a-1a4b1e7c0e88",
			"customer_id":"cust-1",
			"amount_cents":9900,
			"items":[{"sku":"SKU-001","quantity":1}]
		}`),
		OccurredAt: time.Now().UTC(),
	}

	res, err := cli.PublishEvent(context.Background(), evt)
	require.NoError(t, err)
	assert.Equal(t, client.OutcomeAccepted, res.Outcome)

	// verifica que a linha entrou em outbox
	var n int
	require.NoError(t, env.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE event_id=$1`, evt.EventID).Scan(&n))
	assert.Equal(t, 1, n)
}

// TestClientPublishDuplicate — segundo publish com mesmo event_id.
func TestClientPublishDuplicate(t *testing.T) {
	env := newSDKEnv(t)
	defer env.close()

	cli, err := client.New(env.serverURL)
	require.NoError(t, err)

	evt := client.Event{
		EventID:       uuid.New(),
		EventType:     "order.created",
		CorrelationID: "sdk-t12-dup",
		Payload: json.RawMessage(`{
			"order_id":"6c8a1d7f-6e3f-4f13-9b9a-1a4b1e7c0e88",
			"customer_id":"cust-1",
			"amount_cents":9900,
			"items":[{"sku":"SKU-001","quantity":1}]
		}`),
		OccurredAt: time.Now().UTC(),
	}

	res, err := cli.PublishEvent(context.Background(), evt)
	require.NoError(t, err)
	assert.Equal(t, client.OutcomeAccepted, res.Outcome)

	// segundo publish — mesmo event_id
	res2, err := cli.PublishEvent(context.Background(), evt)
	require.NoError(t, err)
	assert.Equal(t, client.OutcomeDuplicate, res2.Outcome,
		"reenvio do mesmo event_id devolve Duplicate (409 tratado como sucesso)")

	// ainda uma única linha em outbox
	var n int
	require.NoError(t, env.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE event_id=$1`, evt.EventID).Scan(&n))
	assert.Equal(t, 1, n)
}

// TestClientPublishValidationFailed — payload viola schema.
func TestClientPublishValidationFailed(t *testing.T) {
	env := newSDKEnv(t)
	defer env.close()

	cli, err := client.New(env.serverURL)
	require.NoError(t, err)

	evt := client.Event{
		EventID:       uuid.New(),
		EventType:     "order.created",
		CorrelationID: "sdk-t12-inv",
		// amount_cents faltando → schema violation
		Payload:    json.RawMessage(`{"order_id":"6c8a1d7f-6e3f-4f13-9b9a-1a4b1e7c0e88","customer_id":"cust-1"}`),
		OccurredAt: time.Now().UTC(),
	}

	_, err = cli.PublishEvent(context.Background(), evt)
	require.Error(t, err)
	assert.True(t, errors.Is(err, client.ErrValidationFailed),
		"errors.Is deve reconhecer ErrValidationFailed")

	var vErr *client.ValidationError
	require.True(t, errors.As(err, &vErr))
	assert.NotEmpty(t, vErr.Body, "body texto do validador acessível")

	// nada foi persistido
	var n int
	require.NoError(t, env.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE event_id=$1`, evt.EventID).Scan(&n))
	assert.Equal(t, 0, n)
}
