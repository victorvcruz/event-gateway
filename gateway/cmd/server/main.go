// Command gateway-server é o binário HTTP: recebe POST /events, valida e
// persiste em gateway_outbox. Quem publica no Kafka é o gateway-worker, em
// outro processo.
package main

import (
	"context"
	"errors"
	"fmt"
	nethttp "net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/gateway/internal/config"
	"github.com/victorvcruz/event-gateway/gateway/internal/ingest"
	"github.com/victorvcruz/event-gateway/gateway/internal/observability"
	"github.com/victorvcruz/event-gateway/gateway/internal/repository"
	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
	transporthttp "github.com/victorvcruz/event-gateway/gateway/internal/transport/http"
	"github.com/victorvcruz/event-gateway/shared/failpoint"
	"github.com/victorvcruz/event-gateway/shared/logging"
	"github.com/victorvcruz/event-gateway/shared/outbox"
)

const ConfigEnv = "EVENT_GATEWAY_CONFIG"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "gateway-server: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	log, err := logging.New(logging.Config{
		ServiceID: "gateway-server",
		Level:     cfg.Observability.Log.Level,
		Encoding:  cfg.Observability.Log.Format,
	})
	if err != nil {
		return err
	}
	defer log.Sync() //nolint:errcheck

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.Database.DSN)
	if err != nil {
		return fmt.Errorf("open pool: %w", err)
	}
	defer pool.Close()

	metrics := observability.New()
	registryClient, err := schema.NewRegistryClient(cfg.SchemaRegistry.Endpoint, cfg.SchemaRegistry.RequestTimeout, metrics)
	if err != nil {
		return fmt.Errorf("build schema registry client: %w", err)
	}
	validator, err := schema.NewRegistryValidator(registryClient, schema.RegistryValidatorConfig{
		GroupName: cfg.SchemaRegistry.GroupName, CacheEnabled: cfg.SchemaRegistry.Cache.Enabled,
		CacheTTL: cfg.SchemaRegistry.Cache.TTL, Observer: metrics,
	})
	if err != nil {
		return fmt.Errorf("build schema validator: %w", err)
	}
	subRegistry := config.NewSubscriptionRegistry(cfg.Subscriptions)
	if err := validator.Warm(ctx, subRegistry.EventTypes()); err != nil {
		return fmt.Errorf("warm schemas from registry: %w", err)
	}
	probe := failpoint.New("gateway-server", log)
	if err := metrics.RegisterPendingGauge(pool, log); err != nil {
		return fmt.Errorf("register pending gauge: %w", err)
	}

	outboxClient := outbox.NewClient(pool, outbox.DefaultTable)
	eventLogRepo := repository.NewEventLog(pool)
	svc := ingest.NewService(pool, outboxClient, eventLogRepo, validator.ForPath("ingest"))

	// /metrics sai numa porta separada, como no worker; por isso o router
	// principal recebe Registry=nil e não expõe a rota.
	deps := transporthttp.Deps{
		Ingest:         svc,
		Registry:       nil,
		IngestLatency:  metrics.IngestLatency,
		EventsReceived: metrics.EventsReceived,
		Log:            log,
		Readiness:      transporthttp.CompositeReadiness{pool, registryClient},
		Probe:          probe,
	}

	server := &nethttp.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:           transporthttp.NewRouter(deps),
		ReadHeaderTimeout: cfg.Server.Timeouts.ReadHeader,
		ReadTimeout:       cfg.Server.Timeouts.Read,
		WriteTimeout:      cfg.Server.Timeouts.Write,
		IdleTimeout:       cfg.Server.Timeouts.Idle,
	}

	metricsSrv := &nethttp.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Observability.Metrics.Port),
		Handler:           metricsMux(metrics),
		ReadHeaderTimeout: 2 * time.Second,
	}

	log.Info("gateway-server listening",
		zap.Int("port", cfg.Server.Port),
		zap.Int("metrics_port", cfg.Observability.Metrics.Port),
	)

	errCh := make(chan error, 2)
	go func() { errCh <- server.ListenAndServe() }()
	go func() { errCh <- metricsSrv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		log.Info("shutting down")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = metricsSrv.Shutdown(shutdownCtx)
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		if err != nil && !errors.Is(err, nethttp.ErrServerClosed) {
			return err
		}
		return nil
	}
}

func metricsMux(m *observability.Metrics) nethttp.Handler {
	mux := nethttp.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	return mux
}

func loadConfig() (*config.Config, error) {
	if path := os.Getenv(ConfigEnv); path != "" {
		return config.Load(path)
	}
	return config.LoadEmbedded()
}
