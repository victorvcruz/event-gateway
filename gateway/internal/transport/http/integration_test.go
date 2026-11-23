package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/gateway/internal/delivery"
	"github.com/victorvcruz/event-gateway/gateway/internal/ingest"
	"github.com/victorvcruz/event-gateway/gateway/internal/observability"
	"github.com/victorvcruz/event-gateway/gateway/internal/publisher"
	"github.com/victorvcruz/event-gateway/gateway/internal/repository"
	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
	transporthttp "github.com/victorvcruz/event-gateway/gateway/internal/transport/http"
	"github.com/victorvcruz/event-gateway/gateway/migrations"
	"github.com/victorvcruz/event-gateway/shared/outbox"
	outboxmig "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
)

type gatewayEnv struct {
	pool      *pgxpool.Pool
	brokers   []string
	serverURL string
	close     func()
}

func newGatewayEnv(ctx context.Context, t *testing.T) *gatewayEnv {
	t.Helper()

	pgContainer, err := tcpostgres.Run(ctx,
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

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	require.NoError(t, outboxmig.Apply(dsn))
	require.NoError(t, outboxmig.Apply(dsn, outboxmig.WithTableName("dispatch_outbox")))
	require.NoError(t, migrations.Apply(dsn))

	kafkaContainer, err := tckafka.Run(ctx,
		"confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("tcc-poc"),
	)
	require.NoError(t, err)

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)

	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	_, err = kadm.NewClient(admin).CreateTopics(ctx, 3, 1, nil, "order.created")
	require.NoError(t, err)
	admin.Close()

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)

	loader := schema.NewLoader()
	require.NoError(t, loader.Load(ctx))

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

	sender, err := publisher.NewKafkaSender(brokers)
	require.NoError(t, err)

	worker, err := outbox.NewWorker(outboxClient, sender, outbox.Config{
		Table:        outbox.DefaultTable,
		BatchSize:    10,
		PollInterval: 50 * time.Millisecond,
		MaxRetries:   3,
	}, zap.NewNop())
	require.NoError(t, err)

	workerCtx, workerCancel := context.WithCancel(ctx)
	go func() { _ = worker.Run(workerCtx) }()

	closeFn := func() {
		workerCancel()
		server.Close()
		sender.Close()
		pool.Close()
		_ = pgContainer.Terminate(context.Background())
		_ = kafkaContainer.Terminate(context.Background())
	}

	return &gatewayEnv{
		pool:      pool,
		brokers:   brokers,
		serverURL: server.URL,
		close:     closeFn,
	}
}

func postEnvelope(t *testing.T, ctx context.Context, url string, envelope map[string]any) *nethttp.Response {
	t.Helper()
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	req, err := nethttp.NewRequestWithContext(ctx, "POST", url+"/events", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func waitStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID uuid.UUID, target string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var status string
		if err := pool.QueryRow(ctx,
			`SELECT status FROM outbox WHERE event_id=$1`, eventID,
		).Scan(&status); err != nil {
			return false
		}
		return status == target
	}, 10*time.Second, 100*time.Millisecond, "outbox deve virar %s", target)
}

// Fluxo completo: POST de envelope válido → 202 → outbox PENDING → o Relay
// publica → status=SENT → mensagem no Kafka com a key correta.
func TestEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	env := newGatewayEnv(ctx, t)
	defer env.close()

	eventID := uuid.New()
	envelope := map[string]any{
		"event_id":       eventID.String(),
		"event_type":     "order.created",
		"correlation_id": "saga-t01",
		"timestamp":      time.Now().UTC().Format(time.RFC3339Nano),
		"payload": map[string]any{
			"order_id":     uuid.NewString(),
			"customer_id":  "cust-1",
			"amount_cents": 19900,
			"items":        []map[string]any{{"sku": "SKU-001", "quantity": 1}},
		},
	}

	resp := postEnvelope(t, ctx, env.serverURL, envelope)
	defer resp.Body.Close()
	require.Equal(t, 202, resp.StatusCode, "POST válido → 202 Accepted")

	waitStatus(t, ctx, env.pool, eventID, "SENT")

	var logDirection string
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT direction FROM event_log WHERE event_id=$1`, eventID,
	).Scan(&logDirection))
	assert.Equal(t, "RECEIVED", logDirection)

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(env.brokers...),
		kgo.ConsumeTopics("order.created"),
		kgo.ConsumerGroup(fmt.Sprintf("t01-%d", time.Now().UnixNano())),
		kgo.DisableAutoCommit(),
	)
	require.NoError(t, err)
	defer consumer.Close()

	fetchCtx, fetchCancel := context.WithTimeout(ctx, 10*time.Second)
	defer fetchCancel()

	var records []*kgo.Record
	for len(records) == 0 {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() || fetchCtx.Err() != nil {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) { records = append(records, r) })
	}
	require.Len(t, records, 1, "exatamente 1 mensagem esperada no Kafka")
	assert.Equal(t, "saga-t01", string(records[0].Key), "a key da mensagem deve ser o correlation_id")
}

// Idempotência: um POST repetido com o mesmo event_id devolve 409 com
// existing_status e não duplica outbox, event_log nem mensagens no Kafka.
func TestDuplicate(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	env := newGatewayEnv(ctx, t)
	defer env.close()

	eventID := uuid.New()
	envelope := map[string]any{
		"event_id":       eventID.String(),
		"event_type":     "order.created",
		"correlation_id": "saga-t02",
		"timestamp":      time.Now().UTC().Format(time.RFC3339Nano),
		"payload": map[string]any{
			"order_id":     uuid.NewString(),
			"customer_id":  "cust-2",
			"amount_cents": 12345,
			"items":        []map[string]any{{"sku": "SKU-001", "quantity": 1}},
		},
	}

	resp1 := postEnvelope(t, ctx, env.serverURL, envelope)
	require.Equal(t, 202, resp1.StatusCode, "primeiro POST → 202")
	resp1.Body.Close()

	waitStatus(t, ctx, env.pool, eventID, "SENT")

	resp2 := postEnvelope(t, ctx, env.serverURL, envelope)
	defer resp2.Body.Close()
	require.Equal(t, 409, resp2.StatusCode, "segundo POST → 409 Conflict")

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&body))
	assert.Equal(t, eventID.String(), body["event_id"], "409 body deve ecoar event_id")
	assert.Equal(t, "SENT", body["existing_status"], "existing_status reflete o estado corrente")

	var outboxCount, eventLogCount int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE event_id=$1`, eventID,
	).Scan(&outboxCount))
	assert.Equal(t, 1, outboxCount, "outbox NÃO deve conter linha duplicada")

	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM event_log WHERE event_id=$1 AND direction='RECEIVED'`, eventID,
	).Scan(&eventLogCount))
	assert.Equal(t, 1, eventLogCount, "event_log NÃO deve conter linha duplicada (409 não grava)")

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(env.brokers...),
		kgo.ConsumeTopics("order.created"),
		kgo.ConsumerGroup(fmt.Sprintf("t02-%d", time.Now().UnixNano())),
		kgo.DisableAutoCommit(),
	)
	require.NoError(t, err)
	defer consumer.Close()

	fetchCtx, fetchCancel := context.WithTimeout(ctx, 8*time.Second)
	defer fetchCancel()

	var records []*kgo.Record
	deadline := time.After(6 * time.Second)
poll:
	for {
		select {
		case <-deadline:
			break poll
		default:
			fetches := consumer.PollFetches(fetchCtx)
			if fetches.IsClientClosed() || fetchCtx.Err() != nil {
				break poll
			}
			fetches.EachRecord(func(r *kgo.Record) { records = append(records, r) })
		}
	}
	require.Len(t, records, 1, "Kafka deve conter exatamente 1 mensagem (POST duplicado NÃO publica)")
	assert.Equal(t, "saga-t02", string(records[0].Key))
}

// failingSender é um outbox.Sender que sempre falha — usado no TestDeadLetter para
// simular indisponibilidade do Kafka sem precisar derrubar o container.
type failingSender struct{ err error }

func (f failingSender) Send(_ context.Context, _ outbox.Row) error { return f.err }

// TestDeadLetter — Kafka indisponível (Sender falha sempre) → POST 202,
// worker esgota MaxRetries → outbox.status='DEAD' + linha em dead_letter +
// gateway_dead_letter_total incrementado (visível em /metrics).
//
// Não sobe container de Kafka: um Sender que sempre falha prova o mesmo
// contrato de "Kafka fora do ar", bem mais barato.
func TestDeadLetter(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	defer func() { _ = pgContainer.Terminate(context.Background()) }()

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	require.NoError(t, outboxmig.Apply(dsn))
	require.NoError(t, outboxmig.Apply(dsn, outboxmig.WithTableName("dispatch_outbox")))
	require.NoError(t, migrations.Apply(dsn))

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	loader := schema.NewLoader()
	require.NoError(t, loader.Load(ctx))

	metrics := observability.New()
	outboxClient := outbox.NewClient(pool, outbox.DefaultTable)
	eventLogRepo := repository.NewEventLog(pool)
	deadLetterRepo := repository.NewDeadLetter(pool)
	sink := delivery.NewDeadLetterSink(outboxClient, deadLetterRepo, metrics.DeadLetterTotal, delivery.ServiceIDKafkaBroker)

	svc := ingest.NewService(pool, outboxClient, eventLogRepo, loader)

	deps := transporthttp.Deps{
		Ingest:         svc,
		Registry:       metrics.Registry,
		IngestLatency:  metrics.IngestLatency,
		EventsReceived: metrics.EventsReceived,
	}
	server := httptest.NewServer(transporthttp.NewRouter(deps))
	defer server.Close()

	worker, err := outbox.NewWorker(outboxClient, failingSender{err: errors.New("kafka: broker unavailable")},
		outbox.Config{
			Table:        outbox.DefaultTable,
			BatchSize:    10,
			PollInterval: 25 * time.Millisecond,
			MaxRetries:   3,
			DeadLetter:   sink.Handle,
		}, zap.NewNop())
	require.NoError(t, err)

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()
	go func() { _ = worker.Run(workerCtx) }()

	eventID := uuid.New()
	envelope := map[string]any{
		"event_id":       eventID.String(),
		"event_type":     "order.created",
		"correlation_id": "saga-t06",
		"timestamp":      time.Now().UTC().Format(time.RFC3339Nano),
		"payload": map[string]any{
			"order_id":     uuid.NewString(),
			"customer_id":  "cust-6",
			"amount_cents": 500,
			"items":        []map[string]any{{"sku": "SKU-001", "quantity": 1}},
		},
	}
	resp := postEnvelope(t, ctx, server.URL, envelope)
	defer resp.Body.Close()
	require.Equal(t, 202, resp.StatusCode)

	// Poll até row entrar em DEAD (5 ciclos × 25ms + margem).
	require.Eventually(t, func() bool {
		var status string
		if err := pool.QueryRow(ctx,
			`SELECT status FROM outbox WHERE event_id=$1`, eventID).Scan(&status); err != nil {
			return false
		}
		return status == "DEAD"
	}, 5*time.Second, 50*time.Millisecond, "outbox deve virar DEAD após MaxRetries falhas")

	// dead_letter deve conter linha com service_id + failure_reason preenchidos.
	var serviceID, reason string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT service_id, failure_reason FROM dead_letter WHERE event_id=$1`, eventID,
	).Scan(&serviceID, &reason))
	assert.Equal(t, delivery.ServiceIDKafkaBroker, serviceID)
	assert.Contains(t, reason, "kafka",
		"failure_reason deve conter o erro do Sender")

	// /metrics deve expor gateway_dead_letter_total{event_type="order.created"} = 1.
	metricsResp, err := nethttp.Get(server.URL + "/metrics")
	require.NoError(t, err)
	defer metricsResp.Body.Close()
	require.Equal(t, 200, metricsResp.StatusCode)

	metricsBody, err := io.ReadAll(metricsResp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(metricsBody),
		`gateway_dead_letter_total{event_type="order.created"} 1`,
		"métrica DeadLetter deve incrementar 1 para order.created")
}

// postRaw envia bytes crus (para cobrir JSON malformado).
func postRaw(t *testing.T, ctx context.Context, url string, body []byte) *nethttp.Response {
	t.Helper()
	req, err := nethttp.NewRequestWithContext(ctx, "POST", url+"/events", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// TestRejections cobre os três casos de rejeição num mesmo ambiente, com um
// sub-teste por cenário:
//
//   - payload inválido → 422 application/problem+json, sem persistência
//   - event_type desconhecido → 422 application/problem+json
//   - JSON quebrado → 400 text/plain
//
// No fim, confere que nada chegou a outbox, event_log nem ao Kafka.
func TestRejections(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	env := newGatewayEnv(ctx, t)
	defer env.close()

	t.Run("payload invalido (schema violation) → 422", func(t *testing.T) {
		envelope := map[string]any{
			"event_id":       uuid.NewString(),
			"event_type":     "order.created",
			"correlation_id": "saga-t03",
			"timestamp":      time.Now().UTC().Format(time.RFC3339Nano),
			"payload": map[string]any{
				// amount_cents negativo viola o schema (minimum: 1).
				"order_id":     uuid.NewString(),
				"customer_id":  "cust-3",
				"amount_cents": -1,
			},
		}
		resp := postEnvelope(t, ctx, env.serverURL, envelope)
		defer resp.Body.Close()
		require.Equal(t, 422, resp.StatusCode)
		assert.Contains(t, resp.Header.Get("Content-Type"), "application/problem+json")
		body, _ := io.ReadAll(resp.Body)
		assert.NotEmpty(t, body, "body deve conter mensagem crua do validador")
	})

	t.Run("event_type desconhecido → 422 schema not found", func(t *testing.T) {
		envelope := map[string]any{
			"event_id":       uuid.NewString(),
			"event_type":     "unknown.event",
			"correlation_id": "saga-t04",
			"timestamp":      time.Now().UTC().Format(time.RFC3339Nano),
			"payload":        map[string]any{"anything": true},
		}
		resp := postEnvelope(t, ctx, env.serverURL, envelope)
		defer resp.Body.Close()
		require.Equal(t, 422, resp.StatusCode)
		assert.Contains(t, resp.Header.Get("Content-Type"), "application/problem+json")
		body, _ := io.ReadAll(resp.Body)
		assert.Contains(t, string(body), "schema_not_found",
			"mensagem específica de event_type sem schema")
		assert.Contains(t, string(body), "unknown.event")
	})

	t.Run("JSON quebrado → 400", func(t *testing.T) {
		resp := postRaw(t, ctx, env.serverURL, []byte(`{"event_id": "abc", broken`))
		defer resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
		assert.Contains(t, resp.Header.Get("Content-Type"), "text/plain")
	})

	// Nada deve ter tocado o banco nem o Kafka.
	var outboxCount, eventLogCount int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox`).Scan(&outboxCount))
	assert.Equal(t, 0, outboxCount, "outbox permanece vazia após rejeições")
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM event_log`).Scan(&eventLogCount))
	assert.Equal(t, 0, eventLogCount, "event_log permanece vazio após rejeições")
}
