package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/victorvcruz/event-gateway/gateway/internal/config"
)

func writeYAML(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

const minimalYAML = `
database:
  dsn: postgres://x:y@z:5432/db?sslmode=disable
kafka:
  brokers: [kafka:9092]
schema_registry:
  endpoint: http://schema-registry:8081
`

func TestLoadDefaults(t *testing.T) {
	path := writeYAML(t, minimalYAML)

	cfg, err := config.Load(path)
	require.NoError(t, err)

	assert.Equal(t, 8080, cfg.Server.Port)
	assert.Equal(t, 2*time.Second, cfg.Server.Timeouts.ReadHeader)
	assert.Equal(t, 5*time.Second, cfg.Server.Timeouts.Read)
	assert.Equal(t, 10*time.Second, cfg.Server.Timeouts.Write)
	assert.Equal(t, 60*time.Second, cfg.Server.Timeouts.Idle)
	assert.Equal(t, 500*time.Millisecond, cfg.Relay.PollInterval)
	assert.Equal(t, 100, cfg.Relay.BatchSize)
	assert.Equal(t, 6, cfg.Relay.MaxRetries)
	assert.Equal(t, time.Second, cfg.Relay.InitialBackoff)
	assert.Equal(t, 30*time.Second, cfg.Relay.MaxBackoff)
	assert.Equal(t, 6, cfg.Dispatcher.Worker.MaxRetries)
	assert.Equal(t, 9090, cfg.Observability.Metrics.Port)
	assert.Equal(t, "info", cfg.Observability.Log.Level)
	assert.Equal(t, "tcc-poc", cfg.SchemaRegistry.GroupName)
	assert.Equal(t, "JSON", cfg.SchemaRegistry.Format)
	assert.Equal(t, "FULL_TRANSITIVE", cfg.SchemaRegistry.Compatibility)
	assert.Equal(t, 2*time.Second, cfg.SchemaRegistry.RequestTimeout)
}

func TestLoadOverridesFromYAML(t *testing.T) {
	path := writeYAML(t, `
server:
  port: 9999
  timeouts:
    read: 3s
database:
  dsn: postgres://x:y@z:5432/db?sslmode=disable
kafka:
  brokers: [kafka:9092]
schema_registry:
  endpoint: http://schema-registry:8081
relay:
  initial_backoff: 250ms
  max_backoff: 4s
`)

	cfg, err := config.Load(path)
	require.NoError(t, err)

	assert.Equal(t, 9999, cfg.Server.Port)
	assert.Equal(t, 3*time.Second, cfg.Server.Timeouts.Read)
	assert.Equal(t, 2*time.Second, cfg.Server.Timeouts.ReadHeader, "default preserved when not overridden")
	assert.Equal(t, 250*time.Millisecond, cfg.Relay.InitialBackoff)
	assert.Equal(t, 4*time.Second, cfg.Relay.MaxBackoff)
}

func TestValidateRelayBackoffRange(t *testing.T) {
	path := writeYAML(t, minimalYAML+`
relay:
  initial_backoff: 2s
  max_backoff: 1s
`)

	_, err := config.Load(path)
	require.Error(t, err)
	assert.ErrorIs(t, err, config.ErrRequiredMissing)
	assert.Contains(t, err.Error(), "relay.max_backoff")
}

func TestValidateMissingDSN(t *testing.T) {
	path := writeYAML(t, `
kafka:
  brokers: [kafka:9092]
`)

	_, err := config.Load(path)
	require.Error(t, err)
	assert.ErrorIs(t, err, config.ErrRequiredMissing)
	assert.Contains(t, err.Error(), "database.dsn")
}

func TestValidateMissingBrokers(t *testing.T) {
	path := writeYAML(t, `
database:
  dsn: postgres://x:y@z:5432/db?sslmode=disable
`)

	_, err := config.Load(path)
	require.Error(t, err)
	assert.ErrorIs(t, err, config.ErrRequiredMissing)
	assert.Contains(t, err.Error(), "kafka.brokers")
}
