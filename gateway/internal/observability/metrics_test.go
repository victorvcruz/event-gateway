package observability_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/victorvcruz/event-gateway/gateway/internal/observability"
	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
)

func TestNewRegistersAllSixCounters(t *testing.T) {
	m := observability.New()
	m.EventsReceived.WithLabelValues("order.created", "accepted").Inc()
	m.EventsPublished.WithLabelValues("order.created", "ack").Inc()
	m.DeadLetterTotal.WithLabelValues("order.created").Inc()
	m.IngestLatency.WithLabelValues("order.created").Observe(0.01)
	m.RelayPublishLatency.WithLabelValues("order.created").Observe(0.02)
	m.ObserveSchemaValidation("ingest", "order.created", "valid", time.Millisecond)
	m.ObserveSchemaCache("hit")
	m.ObserveRegistryRequest("latest", "ok", time.Millisecond)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	for _, name := range []string{
		"gateway_events_received_total",
		"gateway_events_published_total",
		"gateway_ingest_latency_seconds",
		"gateway_relay_publish_latency_seconds",
		"gateway_dead_letter_total",
		"gateway_schema_validations_total",
		"gateway_schema_validation_latency_seconds",
		"gateway_schema_cache_total",
		"gateway_schema_registry_requests_total",
		"gateway_schema_registry_request_latency_seconds",
	} {
		assert.Contains(t, body, name, "expected %s in scrape body", name)
	}
}

func TestSchemaValidationUnknownEventTypesShareBoundedLabel(t *testing.T) {
	m := observability.New()
	for _, eventType := range []string{"attacker.one", "attacker.two", "attacker.three"} {
		m.ObserveSchemaValidation("ingest", eventType, "not_found", time.Millisecond)
	}

	families, err := m.Registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "gateway_schema_validations_total" {
			continue
		}
		require.Len(t, family.Metric, 1)
		for _, label := range family.Metric[0].Label {
			if label.GetName() == "event_type" {
				assert.Equal(t, "unknown", label.GetValue())
				return
			}
		}
	}
	t.Fatal("schema validation event_type label not found")
}

func TestEventTypeLabelsCoverEmbeddedContractCatalog(t *testing.T) {
	catalog, err := schema.LoadEmbeddedCatalog(context.Background())
	require.NoError(t, err)
	for _, eventType := range catalog.EventTypes() {
		assert.Equal(t, eventType, observability.EventTypeLabel(eventType))
	}
}

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

func TestPendingGaugeReflectsRowCount(t *testing.T) {
	dsn, teardown := startPostgres(t)
	defer teardown()

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(ctx, `CREATE TABLE outbox (
		event_id UUID PRIMARY KEY,
		status VARCHAR(20) NOT NULL
	)`)
	require.NoError(t, err)

	m := observability.New()
	require.NoError(t, m.RegisterPendingGauge(pool, nil))

	_, err = pool.Exec(ctx, `INSERT INTO outbox (event_id, status) VALUES
		(gen_random_uuid(), 'PENDING'),
		(gen_random_uuid(), 'PENDING'),
		(gen_random_uuid(), 'SENT')`)
	if err != nil {
		_, _ = pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pgcrypto`)
		_, err = pool.Exec(ctx, `INSERT INTO outbox (event_id, status) VALUES
			(gen_random_uuid(), 'PENDING'),
			(gen_random_uuid(), 'PENDING'),
			(gen_random_uuid(), 'SENT')`)
		require.NoError(t, err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	assert.Contains(t, body, "gateway_outbox_pending_gauge")
	// Extrai o valor numérico da linha do scrape.
	for line := range strings.SplitSeq(body, "\n") {
		if strings.HasPrefix(line, "gateway_outbox_pending_gauge") {
			assert.Contains(t, line, "2")
			return
		}
	}
	t.Fatalf("gauge line not found in body:\n%s", body)
}
