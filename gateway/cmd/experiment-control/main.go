// Command experiment-control é um baseline de dual-write, propositalmente
// inseguro. Existe só para os experimentos quantitativos do TCC e nunca faz
// parte do runtime do gateway nem dos serviços de negócio.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/victorvcruz/event-gateway/gateway/internal/experimentcontrol"
	controlmigrations "github.com/victorvcruz/event-gateway/gateway/internal/experimentcontrol/migrations"
	"github.com/victorvcruz/event-gateway/gateway/internal/publisher"
	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "experiment-control: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	dsn := strings.TrimSpace(os.Getenv("CONTROL_DATABASE_DSN"))
	if dsn == "" {
		return errors.New("CONTROL_DATABASE_DSN is required")
	}
	brokers := splitNonEmpty(os.Getenv("KAFKA_BROKERS"))
	if len(brokers) == 0 {
		return errors.New("KAFKA_BROKERS is required")
	}
	registryURL := strings.TrimSpace(os.Getenv("SCHEMA_REGISTRY_URL"))
	if registryURL == "" {
		return errors.New("SCHEMA_REGISTRY_URL is required")
	}
	group := strings.TrimSpace(os.Getenv("SCHEMA_REGISTRY_GROUP"))
	if group == "" {
		group = "tcc-poc"
	}
	port := 8086
	if raw := strings.TrimSpace(os.Getenv("CONTROL_PORT")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 65535 {
			return fmt.Errorf("invalid CONTROL_PORT %q", raw)
		}
		port = value
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open control database: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping control database: %w", err)
	}
	if err := controlmigrations.Apply(ctx, pool); err != nil {
		return err
	}

	registryClient, err := schema.NewRegistryClient(registryURL, 2*time.Second, nil)
	if err != nil {
		return err
	}
	validator, err := schema.NewRegistryValidator(registryClient, schema.RegistryValidatorConfig{
		GroupName: group, CacheEnabled: true, CacheTTL: 5 * time.Minute,
	})
	if err != nil {
		return err
	}
	if err := validator.Warm(ctx, []string{"order.created", "order.cancelled"}); err != nil {
		return fmt.Errorf("warm experiment schemas: %w", err)
	}
	sender, err := publisher.NewKafkaSender(brokers)
	if err != nil {
		return err
	}
	defer sender.Close()
	app, err := experimentcontrol.New(
		experimentcontrol.NewPostgresStore(pool),
		experimentcontrol.NewKafkaPublisher(sender),
		validator.ForPath("experiment-control"),
		3*time.Second,
	)
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           app.Handler(),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      8 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func splitNonEmpty(raw string) []string {
	var values []string
	for value := range strings.SplitSeq(raw, ",") {
		if value = strings.TrimSpace(value); value != "" {
			values = append(values, value)
		}
	}
	return values
}
