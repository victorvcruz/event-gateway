package experimentcontrol

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	controlmigrations "github.com/victorvcruz/event-gateway/gateway/internal/experimentcontrol/migrations"
	"github.com/victorvcruz/event-gateway/gateway/internal/publisher"
	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
	"github.com/victorvcruz/event-gateway/shared/testpostgres"
)

func TestExperimentControlUsesRealRegistryAndPublishesToKafkaAfterCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	network, err := tcnetwork.New(ctx)
	require.NoError(t, err)
	defer func() { _ = network.Remove(context.Background()) }()
	kafka, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.0",
		tcnetwork.WithNetwork([]string{"kafka"}, network),
		tckafka.WithClusterID("experiment-control-runtime"),
	)
	require.NoError(t, err)
	defer func() { _ = kafka.Terminate(context.Background()) }()
	registryContainer, err := testcontainers.Run(ctx, "confluentinc/cp-schema-registry:7.6.0",
		tcnetwork.WithNetwork([]string{"schema-registry"}, network),
		testcontainers.WithExposedPorts("8081/tcp"),
		testcontainers.WithEnv(map[string]string{
			"SCHEMA_REGISTRY_HOST_NAME":                    "schema-registry",
			"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS": "kafka:9092",
			"SCHEMA_REGISTRY_LISTENERS":                    "http://0.0.0.0:8081",
		}),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/subjects").WithPort("8081/tcp").WithStartupTimeout(90*time.Second)),
	)
	require.NoError(t, err)
	defer func() { _ = registryContainer.Terminate(context.Background()) }()
	endpoint, err := registryContainer.Endpoint(ctx, "http")
	require.NoError(t, err)
	registryClient, err := schema.NewRegistryClient(endpoint, 3*time.Second, nil)
	require.NoError(t, err)
	catalog, err := schema.LoadEmbeddedCatalog(ctx)
	require.NoError(t, err)
	provisioner, err := schema.NewProvisioner(registryClient, "tcc-poc", "FULL_TRANSITIVE")
	require.NoError(t, err)
	_, err = provisioner.Provision(ctx, catalog, catalog.EventTypes())
	require.NoError(t, err)
	validator, err := schema.NewRegistryValidator(registryClient, schema.RegistryValidatorConfig{
		GroupName: "tcc-poc", CacheEnabled: true, CacheTTL: time.Minute,
	})
	require.NoError(t, err)

	postgresHarness := testpostgres.Start(t)
	require.NoError(t, controlmigrations.Apply(ctx, postgresHarness.Pool))
	brokers, err := kafka.Brokers(ctx)
	require.NoError(t, err)
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	_, err = kadm.NewClient(adminClient).CreateTopics(ctx, 1, 1, nil, "order.created")
	require.NoError(t, err)
	adminClient.Close()
	sender, err := publisher.NewKafkaSender(brokers)
	require.NoError(t, err)
	defer sender.Close()
	app, err := New(NewPostgresStore(postgresHarness.Pool), NewKafkaPublisher(sender), validator.ForPath("experiment-control"), 5*time.Second)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	request := directRequest()
	app.Handler().ServeHTTP(recorder, request)
	require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())

	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics("order.created"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer consumer.Close()
	records := consumer.PollRecords(ctx, 1)
	require.NoError(t, records.Err())
	require.Len(t, records.Records(), 1)

	invalid := `{"event_id":"` + uuid.NewString() + `","event_type":"order.cancelled","correlation_id":"run","timestamp":"2026-08-28T00:00:00Z","payload":{"order_id":"` + uuid.NewString() + `","reason":"not-in-contract"}}`
	recorder = httptest.NewRecorder()
	app.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/benchmark/schema", strings.NewReader(invalid)))
	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
}
