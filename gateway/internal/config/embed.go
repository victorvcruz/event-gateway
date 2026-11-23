package config

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/spf13/viper"

	"github.com/victorvcruz/event-gateway/gateway"
)

// LoadEmbedded devolve a config embarcada (gateway/config.local.yaml). É o
// fallback quando $EVENT_GATEWAY_CONFIG não está definido.
func LoadEmbedded() (*Config, error) {
	v := viper.New()
	applyDefaults(v)

	v.SetConfigType("yaml")
	if err := v.ReadConfig(bytes.NewReader(gateway.ConfigLocalYAML)); err != nil {
		return nil, fmt.Errorf("read embedded config: %w", err)
	}

	v.SetEnvPrefix("GATEWAY")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal embedded config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}
