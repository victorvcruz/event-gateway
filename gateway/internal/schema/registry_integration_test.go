package schema

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startSchemaRegistry(t *testing.T) (*RegistryClient, func()) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test requires Docker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	network, err := tcnetwork.New(ctx)
	require.NoError(t, err)
	kafka, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.0",
		tcnetwork.WithNetwork([]string{"kafka"}, network),
		tckafka.WithClusterID("schema-registry-tests"),
	)
	require.NoError(t, err)
	registry, err := testcontainers.Run(ctx, "confluentinc/cp-schema-registry:7.6.0",
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
	endpoint, err := registry.Endpoint(ctx, "http")
	require.NoError(t, err)
	client, err := NewRegistryClient(endpoint, 3*time.Second, nil)
	require.NoError(t, err)
	return client, func() {
		_ = registry.Terminate(context.Background())
		_ = kafka.Terminate(context.Background())
		_ = network.Remove(context.Background())
	}
}

func TestSchemaRegistryPublishesNineContractsIdempotently(t *testing.T) {
	client, cleanup := startSchemaRegistry(t)
	defer cleanup()
	catalog, err := LoadEmbeddedCatalog(context.Background())
	require.NoError(t, err)
	provisioner, err := NewProvisioner(client, "tcc-poc", "FULL_TRANSITIVE")
	require.NoError(t, err)

	first, err := provisioner.Provision(context.Background(), catalog, catalog.EventTypes())
	require.NoError(t, err)
	require.Len(t, first, 9)
	for _, result := range first {
		assert.True(t, result.Created)
		assert.Equal(t, 1, result.Version)
		latest, latestErr := client.Latest(context.Background(), result.Subject)
		require.NoError(t, latestErr)
		assert.Equal(t, "JSON", latest.Type)
		level, compatibilityErr := client.Compatibility(context.Background(), result.Subject)
		require.NoError(t, compatibilityErr)
		assert.Equal(t, "FULL_TRANSITIVE", level)
	}
	second, err := provisioner.Provision(context.Background(), catalog, catalog.EventTypes())
	require.NoError(t, err)
	for _, result := range second {
		assert.False(t, result.Created)
		assert.Equal(t, 1, result.Version)
	}
	subjects, err := client.Subjects(context.Background())
	require.NoError(t, err)
	assert.Len(t, subjects, 9)
}

func TestSchemaRegistryCompatibilityModesAndTransitiveHistory(t *testing.T) {
	client, cleanup := startSchemaRegistry(t)
	defer cleanup()
	baseOpen := []byte(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":true}`)
	requiresName := []byte(`{"type":"object","properties":{"id":{"type":"string"},"name":{"type":"string"}},"required":["id","name"],"additionalProperties":true}`)
	baseClosed := []byte(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false}`)
	addsName := []byte(`{"type":"object","properties":{"id":{"type":"string"},"name":{"type":"string"}},"required":["id"],"additionalProperties":false}`)
	baseOpenDocumented := []byte(`{"title":"documented","type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":true}`)
	anyString := []byte(`{"type":"string"}`)
	onlyAEnum := []byte(`{"type":"string","enum":["A"]}`)

	check := func(mode string, base, candidate []byte, expected bool) {
		t.Helper()
		subject := fmt.Sprintf("compat-%s-%s-value", mode, uuid.NewString())
		require.NoError(t, client.SetCompatibility(context.Background(), subject, mode))
		_, err := client.Register(context.Background(), subject, base)
		require.NoError(t, err)
		compatible, _, err := client.CheckCompatibilityLatest(context.Background(), subject, candidate)
		require.NoError(t, err)
		assert.Equal(t, expected, compatible, mode)
	}
	check("BACKWARD", baseOpen, requiresName, false)
	check("BACKWARD", baseOpen, baseOpenDocumented, true)
	check("FORWARD", baseClosed, addsName, false)
	check("FULL", baseClosed, addsName, false)

	subject := "compat-transitive-" + uuid.NewString() + "-value"
	require.NoError(t, client.SetCompatibility(context.Background(), subject, "NONE"))
	_, err := client.Register(context.Background(), subject, anyString)
	require.NoError(t, err)
	_, err = client.Register(context.Background(), subject, onlyAEnum)
	require.NoError(t, err)
	require.NoError(t, client.SetCompatibility(context.Background(), subject, "FULL"))
	compatible, _, err := client.CheckCompatibilityLatest(context.Background(), subject, onlyAEnum)
	require.NoError(t, err)
	assert.True(t, compatible)
	require.NoError(t, client.SetCompatibility(context.Background(), subject, "FULL_TRANSITIVE"))
	compatible, _, err = client.CheckCompatibility(context.Background(), subject, onlyAEnum)
	require.NoError(t, err)
	assert.False(t, compatible, "FULL_TRANSITIVE must compare against version 1, not only latest")
}
