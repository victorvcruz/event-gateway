package consumer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/gateway/internal/consumer"
	"github.com/victorvcruz/event-gateway/gateway/internal/ingest"
	"github.com/victorvcruz/event-gateway/gateway/internal/observability"
	"github.com/victorvcruz/event-gateway/gateway/internal/repository"
	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
	transporthttp "github.com/victorvcruz/event-gateway/gateway/internal/transport/http"
	gwmigrations "github.com/victorvcruz/event-gateway/gateway/migrations"
	"github.com/victorvcruz/event-gateway/shared/outbox"
	sharedmig "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
)

type runtimeSubscriptions map[string][]consumer.Subscriber

func (r runtimeSubscriptions) SubscriptionsOf(eventType string) []consumer.Subscriber {
	return r[eventType]
}

func registryRuntimeCatalog(t *testing.T) schema.Catalog {
	t.Helper()
	catalog, err := schema.LoadEmbeddedCatalog(context.Background())
	require.NoError(t, err)

	var document map[string]any
	require.NoError(t, json.Unmarshal(catalog["order.created"], &document))
	properties := document["properties"].(map[string]any)
	payload := properties["payload"].(map[string]any)
	payloadProperties := payload["properties"].(map[string]any)
	payloadProperties["registry_test_marker"] = map[string]any{"type": "boolean", "const": true}
	payload["required"] = append(payload["required"].([]any), "registry_test_marker")
	catalog["order.created"], err = json.Marshal(document)
	require.NoError(t, err)
	return catalog
}

func registryRuntimeEnvelope(eventID uuid.UUID, includeMarker bool) []byte {
	payload := map[string]any{
		"order_id": uuid.NewString(), "customer_id": "registry-runtime",
		"amount_cents": 1000,
		"items":        []map[string]any{{"sku": "SKU-001", "quantity": 1}},
	}
	if includeMarker {
		payload["registry_test_marker"] = true
	}
	body, _ := json.Marshal(map[string]any{
		"event_id": eventID, "event_type": "order.created",
		"correlation_id": eventID.String(), "timestamp": time.Now().UTC(), "payload": payload,
	})
	return body
}

// Este teste monta os mesmos caminhos de RegistryValidator usados pelo
// gateway-server e pelo gateway-worker. O schema do teste exige um marcador que
// o schema embarcado rejeitaria, o que prova que os dois caminhos leem do
// Registry.
func TestRegistryValidatorWiresHTTPIngestAndKafkaConsumer(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	network, err := tcnetwork.New(ctx)
	require.NoError(t, err)
	kafkaContainer, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tcnetwork.WithNetwork([]string{"kafka"}, network),
		tckafka.WithClusterID("registry-runtime-wiring"),
	)
	require.NoError(t, err)
	registryContainer, err := testcontainers.Run(ctx, "confluentinc/cp-schema-registry:7.6.0",
		tcnetwork.WithNetwork([]string{"schema-registry"}, network),
		testcontainers.WithExposedPorts("8081/tcp"),
		testcontainers.WithEnv(map[string]string{
			"SCHEMA_REGISTRY_HOST_NAME":                    "schema-registry",
			"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS": "kafka:9092",
			"SCHEMA_REGISTRY_LISTENERS":                    "http://0.0.0.0:8081",
		}),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/subjects").WithPort("8081/tcp").WithStartupTimeout(60*time.Second)),
	)
	require.NoError(t, err)
	postgresContainer, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("gateway_db"), tcpostgres.WithUsername("gateway"), tcpostgres.WithPassword("gateway"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(30*time.Second)),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = postgresContainer.Terminate(context.Background())
		_ = registryContainer.Terminate(context.Background())
		_ = kafkaContainer.Terminate(context.Background())
		_ = network.Remove(context.Background())
	})

	dsn, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	require.NoError(t, sharedmig.Apply(dsn))
	require.NoError(t, sharedmig.Apply(dsn, sharedmig.WithTableName("dispatch_outbox")))
	require.NoError(t, gwmigrations.Apply(dsn))
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	registryEndpoint, err := registryContainer.Endpoint(ctx, "http")
	require.NoError(t, err)
	metrics := observability.New()
	registryClient, err := schema.NewRegistryClient(registryEndpoint, 3*time.Second, metrics)
	require.NoError(t, err)
	groupName := "runtime-" + uuid.NewString()
	provisioner, err := schema.NewProvisioner(registryClient, groupName, "FULL_TRANSITIVE")
	require.NoError(t, err)
	catalog := registryRuntimeCatalog(t)
	results, err := provisioner.Provision(ctx, catalog, catalog.EventTypes())
	require.NoError(t, err)
	require.Len(t, results, 9)
	validator, err := schema.NewRegistryValidator(registryClient, schema.RegistryValidatorConfig{
		GroupName: groupName, CacheEnabled: true, CacheTTL: time.Minute, Observer: metrics,
	})
	require.NoError(t, err)
	require.NoError(t, validator.Warm(ctx, catalog.EventTypes()))

	outboxClient := outbox.NewClient(pool, outbox.DefaultTable)
	ingestService := ingest.NewService(pool, outboxClient, repository.NewEventLog(pool), validator.ForPath("ingest"))
	server := httptest.NewServer(transporthttp.NewRouter(transporthttp.Deps{
		Ingest: ingestService, IngestLatency: metrics.IngestLatency, EventsReceived: metrics.EventsReceived,
	}))
	defer server.Close()

	httpEventID := uuid.New()
	response, err := http.Post(server.URL+"/events", "application/json", bytes.NewReader(registryRuntimeEnvelope(httpEventID, true)))
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, response.StatusCode)
	response.Body.Close()
	var httpRows int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE event_id=$1", httpEventID).Scan(&httpRows))
	require.Equal(t, 1, httpRows)

	response, err = http.Post(server.URL+"/events", "application/json", bytes.NewReader(registryRuntimeEnvelope(uuid.New(), false)))
	require.NoError(t, err)
	require.Equal(t, http.StatusUnprocessableEntity, response.StatusCode)
	require.Equal(t, "application/problem+json", response.Header.Get("Content-Type"))
	response.Body.Close()

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer admin.Close()
	_, err = kadm.NewClient(admin).CreateTopics(ctx, 1, 1, nil, "order.created")
	require.NoError(t, err)

	subscriptions := runtimeSubscriptions{"order.created": {{ID: "payment-service", CallbackURL: "http://payment/dispatch"}}}
	kafkaConsumer, err := consumer.New(consumer.Config{
		Brokers: brokers, GroupID: "registry-runtime-" + uuid.NewString(), EventTypes: []string{"order.created"},
	}, subscriptions, pool, repository.NewDispatch(pool), validator.ForPath("consumer"), repository.NewDeadLetter(pool),
		consumer.Metrics{Consumed: metrics.DispatchConsumed}, zap.NewNop())
	require.NoError(t, err)
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	defer stopConsumer()
	defer kafkaConsumer.Close()
	go func() { _ = kafkaConsumer.Run(consumerCtx) }()

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()))
	require.NoError(t, err)
	defer producer.Close()
	kafkaValidID, kafkaInvalidID := uuid.New(), uuid.New()
	require.NoError(t, producer.ProduceSync(ctx,
		&kgo.Record{Topic: "order.created", Value: registryRuntimeEnvelope(kafkaValidID, true)},
		&kgo.Record{Topic: "order.created", Value: registryRuntimeEnvelope(kafkaInvalidID, false)},
	).FirstErr())

	require.Eventually(t, func() bool {
		var dispatchRows, deadRows int
		if pool.QueryRow(ctx, "SELECT count(*) FROM dispatch_outbox WHERE event_id=$1", kafkaValidID).Scan(&dispatchRows) != nil {
			return false
		}
		if pool.QueryRow(ctx, "SELECT count(*) FROM dead_letter WHERE event_id=$1 AND failure_code='schema_invalid'", kafkaInvalidID).Scan(&deadRows) != nil {
			return false
		}
		return dispatchRows == 1 && deadRows == 1
	}, 30*time.Second, 200*time.Millisecond)
}
