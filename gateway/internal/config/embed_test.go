package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/victorvcruz/event-gateway/gateway/internal/config"
)

func TestLoadEmbeddedParsesAndValidates(t *testing.T) {
	cfg, err := config.LoadEmbedded()
	require.NoError(t, err)

	// Valores esperados do config.local.yaml embarcado.
	assert.Equal(t, 8080, cfg.Server.Port)
	assert.Equal(t, 2*time.Second, cfg.Server.Timeouts.ReadHeader)
	assert.Equal(t, 5*time.Second, cfg.Server.Timeouts.Read)
	assert.Equal(t, 10*time.Second, cfg.Server.Timeouts.Write)
	assert.Equal(t, 60*time.Second, cfg.Server.Timeouts.Idle)

	assert.Contains(t, cfg.Database.DSN, "gateway-db:5432")
	assert.Contains(t, cfg.Database.DSN, "gateway_db")

	require.Len(t, cfg.Kafka.Brokers, 1)
	assert.Equal(t, "kafka:9092", cfg.Kafka.Brokers[0])

	assert.Equal(t, 500*time.Millisecond, cfg.Relay.PollInterval)
	assert.Equal(t, 100, cfg.Relay.BatchSize)
	assert.Equal(t, 6, cfg.Relay.MaxRetries)
	assert.Equal(t, time.Second, cfg.Relay.InitialBackoff)
	assert.Equal(t, 30*time.Second, cfg.Relay.MaxBackoff)
	assert.Equal(t, 6, cfg.Dispatcher.Worker.MaxRetries)

	assert.Equal(t, 9090, cfg.Observability.Metrics.Port)
}

func TestLoadEmbeddedRespectsEnvOverride(t *testing.T) {
	t.Setenv("GATEWAY_DATABASE_DSN", "postgres://x:y@127.0.0.1:5432/other?sslmode=disable")

	cfg, err := config.LoadEmbedded()
	require.NoError(t, err)

	assert.Equal(t, "postgres://x:y@127.0.0.1:5432/other?sslmode=disable", cfg.Database.DSN)
}
