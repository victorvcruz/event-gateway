// Package config carrega o gateway-config.yaml via Viper numa struct tipada.
// Os defaults ficam no código e o YAML sobrescreve. Campo obrigatório ausente
// derruba o boot.
package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server         Server             `mapstructure:"server"`
	Database       Database           `mapstructure:"database"`
	Kafka          Kafka              `mapstructure:"kafka"`
	Relay          Relay              `mapstructure:"relay"`
	Dispatcher     Dispatcher         `mapstructure:"dispatcher"`
	SchemaRegistry SchemaRegistry     `mapstructure:"schema_registry"`
	Observability  Observability      `mapstructure:"observability"`
	Subscriptions  []SubscriptionRule `mapstructure:"subscriptions"`
}

type SubscriptionRule struct {
	EventType string             `mapstructure:"event_type"`
	Consumers []SubscriberConfig `mapstructure:"consumers"`
}

type SubscriberConfig struct {
	ID  string `mapstructure:"id"`
	URL string `mapstructure:"url"`
}

type Server struct {
	Port     int      `mapstructure:"port"`
	Timeouts Timeouts `mapstructure:"timeouts"`
}

type Timeouts struct {
	ReadHeader time.Duration `mapstructure:"read_header"`
	Read       time.Duration `mapstructure:"read"`
	Write      time.Duration `mapstructure:"write"`
	Idle       time.Duration `mapstructure:"idle"`
}

type Database struct {
	DSN             string        `mapstructure:"dsn"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

type Kafka struct {
	Brokers  []string      `mapstructure:"brokers"`
	Producer KafkaProducer `mapstructure:"producer"`
}

type KafkaProducer struct {
	Acks         string        `mapstructure:"acks"`
	Retries      int           `mapstructure:"retries"`
	RetryBackoff time.Duration `mapstructure:"retry_backoff"`
	Compression  string        `mapstructure:"compression"`
}

type Relay struct {
	PollInterval   time.Duration `mapstructure:"poll_interval"`
	BatchSize      int           `mapstructure:"batch_size"`
	InitialBackoff time.Duration `mapstructure:"initial_backoff"`
	MaxBackoff     time.Duration `mapstructure:"max_backoff"`
	MaxRetries     int           `mapstructure:"max_retries"`
}

type Dispatcher struct {
	Worker   DispatcherWorker   `mapstructure:"worker"`
	Consumer DispatcherConsumer `mapstructure:"consumer"`
	HTTP     DispatcherHTTP     `mapstructure:"http"`
}

type DispatcherWorker struct {
	BatchSize    int               `mapstructure:"batch_size"`
	PollInterval time.Duration     `mapstructure:"poll_interval"`
	MaxRetries   int               `mapstructure:"max_retries"`
	Backoff      DispatcherBackoff `mapstructure:"backoff"`
}

type DispatcherBackoff struct {
	Base time.Duration `mapstructure:"base"`
}

type DispatcherConsumer struct {
	GroupID        string        `mapstructure:"group_id"`
	SessionTimeout time.Duration `mapstructure:"session_timeout"`
}

type DispatcherHTTP struct {
	PerAttemptTimeout time.Duration `mapstructure:"per_attempt_timeout"`
}

type SchemaRegistry struct {
	Endpoint  string `mapstructure:"endpoint"`
	GroupName string `mapstructure:"group_name"`
	Format    string `mapstructure:"format"`
	// Os nove contratos operacionais usam sempre FULL_TRANSITIVE. Outros
	// modos só existem no experimento.
	Compatibility  string        `mapstructure:"compatibility"`
	RequestTimeout time.Duration `mapstructure:"request_timeout"`
	Cache          SchemaCache   `mapstructure:"cache"`
}

type SchemaCache struct {
	Enabled bool          `mapstructure:"enabled"`
	TTL     time.Duration `mapstructure:"ttl"`
}

type Observability struct {
	Metrics MetricsCfg `mapstructure:"metrics"`
	Log     LogCfg     `mapstructure:"log"`
}

type MetricsCfg struct {
	Enabled bool `mapstructure:"enabled"`
	Port    int  `mapstructure:"port"`
}

type LogCfg struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

func Load(path string) (*Config, error) {
	v := viper.New()
	applyDefaults(v)

	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("read config %q: %w", path, err)
		}
	}

	v.SetEnvPrefix("GATEWAY")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func applyDefaults(v *viper.Viper) {
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.timeouts.read_header", 2*time.Second)
	v.SetDefault("server.timeouts.read", 5*time.Second)
	v.SetDefault("server.timeouts.write", 10*time.Second)
	v.SetDefault("server.timeouts.idle", 60*time.Second)

	v.SetDefault("database.max_open_conns", 20)
	v.SetDefault("database.max_idle_conns", 5)
	v.SetDefault("database.conn_max_lifetime", 30*time.Minute)

	v.SetDefault("kafka.producer.acks", "all")
	v.SetDefault("kafka.producer.retries", 5)
	v.SetDefault("kafka.producer.retry_backoff", 100*time.Millisecond)
	v.SetDefault("kafka.producer.compression", "snappy")

	v.SetDefault("relay.poll_interval", 500*time.Millisecond)
	v.SetDefault("relay.batch_size", 100)
	v.SetDefault("relay.max_retries", 6)
	v.SetDefault("relay.initial_backoff", time.Second)
	v.SetDefault("relay.max_backoff", 30*time.Second)

	v.SetDefault("dispatcher.worker.batch_size", 100)
	v.SetDefault("dispatcher.worker.poll_interval", 500*time.Millisecond)
	v.SetDefault("dispatcher.worker.max_retries", 6)
	v.SetDefault("dispatcher.worker.backoff.base", 1*time.Second)
	v.SetDefault("dispatcher.consumer.group_id", "gateway-dispatcher")
	v.SetDefault("dispatcher.consumer.session_timeout", 10*time.Second)
	v.SetDefault("dispatcher.http.per_attempt_timeout", 5*time.Second)

	v.SetDefault("schema_registry.cache.enabled", true)
	v.SetDefault("schema_registry.cache.ttl", 5*time.Minute)
	v.SetDefault("schema_registry.group_name", "tcc-poc")
	v.SetDefault("schema_registry.format", "JSON")
	v.SetDefault("schema_registry.compatibility", "FULL_TRANSITIVE")
	v.SetDefault("schema_registry.request_timeout", 2*time.Second)

	v.SetDefault("observability.metrics.enabled", true)
	v.SetDefault("observability.metrics.port", 9090)
	v.SetDefault("observability.log.level", "info")
	v.SetDefault("observability.log.format", "json")
}

var ErrRequiredMissing = errors.New("required config missing")

func (c *Config) Validate() error {
	if c.Database.DSN == "" {
		return fmt.Errorf("%w: database.dsn", ErrRequiredMissing)
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("%w: kafka.brokers", ErrRequiredMissing)
	}
	if c.Server.Port <= 0 {
		return fmt.Errorf("%w: server.port must be > 0", ErrRequiredMissing)
	}
	if c.Observability.Metrics.Port <= 0 {
		return fmt.Errorf("%w: observability.metrics.port must be > 0", ErrRequiredMissing)
	}
	if c.Relay.BatchSize <= 0 || c.Relay.MaxRetries < 0 || c.Relay.PollInterval <= 0 {
		return fmt.Errorf("%w: relay.* invalid", ErrRequiredMissing)
	}
	if c.Relay.InitialBackoff <= 0 {
		return fmt.Errorf("%w: relay.initial_backoff must be > 0", ErrRequiredMissing)
	}
	if c.Relay.MaxBackoff < c.Relay.InitialBackoff {
		return fmt.Errorf("%w: relay.max_backoff must be >= relay.initial_backoff", ErrRequiredMissing)
	}
	if c.Dispatcher.Worker.BatchSize <= 0 || c.Dispatcher.Worker.MaxRetries < 0 || c.Dispatcher.Worker.PollInterval <= 0 {
		return fmt.Errorf("%w: dispatcher.worker.* invalid", ErrRequiredMissing)
	}
	if c.Dispatcher.Worker.Backoff.Base <= 0 {
		return fmt.Errorf("%w: dispatcher.worker.backoff.base must be > 0", ErrRequiredMissing)
	}
	if c.Dispatcher.Consumer.GroupID == "" {
		return fmt.Errorf("%w: dispatcher.consumer.group_id", ErrRequiredMissing)
	}
	if c.Dispatcher.Consumer.SessionTimeout <= 0 {
		return fmt.Errorf("%w: dispatcher.consumer.session_timeout must be > 0", ErrRequiredMissing)
	}
	if c.Dispatcher.HTTP.PerAttemptTimeout <= 0 {
		return fmt.Errorf("%w: dispatcher.http.per_attempt_timeout must be > 0", ErrRequiredMissing)
	}
	if c.SchemaRegistry.Endpoint == "" {
		return fmt.Errorf("%w: schema_registry.endpoint", ErrRequiredMissing)
	}
	if c.SchemaRegistry.GroupName == "" {
		return fmt.Errorf("%w: schema_registry.group_name", ErrRequiredMissing)
	}
	if c.SchemaRegistry.Format != "JSON" {
		return fmt.Errorf("%w: schema_registry.format must be JSON", ErrRequiredMissing)
	}
	if c.SchemaRegistry.Compatibility != "FULL_TRANSITIVE" {
		return fmt.Errorf("%w: schema_registry.compatibility must be FULL_TRANSITIVE", ErrRequiredMissing)
	}
	if c.SchemaRegistry.RequestTimeout <= 0 {
		return fmt.Errorf("%w: schema_registry.request_timeout must be > 0", ErrRequiredMissing)
	}
	if c.SchemaRegistry.Cache.Enabled && c.SchemaRegistry.Cache.TTL <= 0 {
		return fmt.Errorf("%w: schema_registry.cache.ttl must be > 0", ErrRequiredMissing)
	}
	return nil
}
