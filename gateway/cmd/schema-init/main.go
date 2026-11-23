// Command schema-init publica no Confluent Schema Registry o catálogo de JSON
// Schemas versionado no Git. É o único processo de runtime com permissão
// administrativa sobre subjects e configurações de compatibilidade.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/victorvcruz/event-gateway/gateway/internal/config"
	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
)

const configEnv = "EVENT_GATEWAY_CONFIG"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "schema-init: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	client, err := schema.NewRegistryClient(cfg.SchemaRegistry.Endpoint, cfg.SchemaRegistry.RequestTimeout, nil)
	if err != nil {
		return err
	}
	catalog, err := schema.LoadEmbeddedCatalog(ctx)
	if err != nil {
		return err
	}
	registry := config.NewSubscriptionRegistry(cfg.Subscriptions)
	provisioner, err := schema.NewProvisioner(client, cfg.SchemaRegistry.GroupName, cfg.SchemaRegistry.Compatibility)
	if err != nil {
		return err
	}
	results, err := provisioner.Provision(ctx, catalog, registry.EventTypes())
	if err != nil {
		return err
	}
	for _, result := range results {
		fmt.Printf("subject=%s event_type=%s id=%d version=%d created=%t\n", result.Subject, result.EventType, result.ID, result.Version, result.Created)
	}
	return nil
}

func loadConfig() (*config.Config, error) {
	if path := os.Getenv(configEnv); path != "" {
		return config.Load(path)
	}
	return config.LoadEmbedded()
}
