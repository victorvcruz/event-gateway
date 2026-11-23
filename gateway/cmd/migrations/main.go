// Command migrations aplica as migrations embarcadas em gateway/migrations
// no banco descrito pelo YAML de $EVENT_GATEWAY_CONFIG. Sem a variável, usa a
// config embarcada no binário (config.local.yaml), o que dispensa volume no
// docker-compose. Não recebe flags e pode rodar quantas vezes for preciso.
package main

import (
	"fmt"
	"os"

	"github.com/victorvcruz/event-gateway/gateway/internal/config"
	"github.com/victorvcruz/event-gateway/gateway/migrations"
	outboxmig "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
)

const ConfigEnv = "EVENT_GATEWAY_CONFIG"

func main() {
	if err := runFromEnv(); err != nil {
		fmt.Fprintf(os.Stderr, "migrations: %v\n", err)
		os.Exit(1)
	}
}

func runFromEnv() error {
	var (
		cfg *config.Config
		err error
	)
	if path := os.Getenv(ConfigEnv); path != "" {
		cfg, err = config.Load(path)
	} else {
		cfg, err = config.LoadEmbedded()
	}
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if err := outboxmig.Apply(cfg.Database.DSN); err != nil {
		return fmt.Errorf("outbox migrations: %w", err)
	}
	if err := outboxmig.Apply(cfg.Database.DSN, outboxmig.WithTableName("dispatch_outbox")); err != nil {
		return fmt.Errorf("dispatch_outbox base migrations: %w", err)
	}
	if err := migrations.Apply(cfg.Database.DSN); err != nil {
		return fmt.Errorf("gateway migrations: %w", err)
	}
	return nil
}
