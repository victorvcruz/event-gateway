// Command gateway-worker roda o Relay: lê gateway_outbox e publica no Kafka
// via franz-go. Fica separado do gateway-server para que uma queda do HTTP não
// pare a publicação.
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
	"github.com/victorvcruz/event-gateway/gateway/internal/consumer"
	"github.com/victorvcruz/event-gateway/gateway/internal/delivery"
	"github.com/victorvcruz/event-gateway/gateway/internal/observability"
	"github.com/victorvcruz/event-gateway/gateway/internal/publisher"
	"github.com/victorvcruz/event-gateway/gateway/internal/repository"
	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
	"github.com/victorvcruz/event-gateway/shared/failpoint"
	"github.com/victorvcruz/event-gateway/shared/logging"
	"github.com/victorvcruz/event-gateway/shared/outbox"
)

const ConfigEnv = "EVENT_GATEWAY_CONFIG"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "gateway-worker: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	log, err := logging.New(logging.Config{
		ServiceID: "gateway-worker",
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

	sender, err := publisher.NewKafkaSender(cfg.Kafka.Brokers)
	if err != nil {
		return fmt.Errorf("kafka sender: %w", err)
	}
	sender.WithLogger(log)
	defer sender.Close()

	metrics := observability.New()
	probe := failpoint.New("gateway-worker", log)
	if err := metrics.RegisterPendingGauge(pool, log); err != nil {
		return fmt.Errorf("register pending gauge: %w", err)
	}
	if err := metrics.RegisterDispatchPendingGauge(pool, log); err != nil {
		return fmt.Errorf("register dispatch pending gauge: %w", err)
	}

	client := outbox.NewClient(pool, outbox.DefaultTable)
	deadLetterRepo := repository.NewDeadLetter(pool)
	dispatchRepo := repository.NewDispatch(pool)
	sink := delivery.NewDeadLetterSink(client, deadLetterRepo, metrics.DeadLetterTotal, delivery.ServiceIDKafkaBroker).
		WithDispatch(dispatchRepo, metrics.DispatchDeadLetter)

	worker, err := outbox.NewWorker(client, sender, outbox.Config{
		Table:           outbox.DefaultTable,
		BatchSize:       cfg.Relay.BatchSize,
		PollInterval:    cfg.Relay.PollInterval,
		MaxRetries:      cfg.Relay.MaxRetries,
		BackoffSchedule: outbox.BackoffExponentialCapped(cfg.Relay.InitialBackoff, cfg.Relay.MaxBackoff),
		DeadLetter:      sink.Handle,
		Probe:           probe,
	}, log)
	if err != nil {
		return fmt.Errorf("build worker: %w", err)
	}

	dispatchClient := outbox.NewClient(pool, "dispatch_outbox", outbox.WithServiceIDDeliveryKey())
	httpDoer := &nethttp.Client{Timeout: cfg.Dispatcher.HTTP.PerAttemptTimeout}
	httpSender := delivery.NewHTTPSender(httpDoer).WithLogger(log).WithDispatchToken(os.Getenv("DISPATCH_TOKEN"))
	successHook := delivery.NewSuccessHookEventLog(dispatchRepo, metrics.DispatchLatency)

	dispatchWorker, err := outbox.NewWorker(dispatchClient, httpSender, outbox.Config{
		Table:           "dispatch_outbox",
		BatchSize:       cfg.Dispatcher.Worker.BatchSize,
		PollInterval:    cfg.Dispatcher.Worker.PollInterval,
		MaxRetries:      cfg.Dispatcher.Worker.MaxRetries,
		BackoffSchedule: outbox.BackoffExponential(cfg.Dispatcher.Worker.Backoff.Base),
		Success:         successHook.Handle,
		DeadLetter:      sink.Handle,
		Probe:           probe,
	}, log)
	if err != nil {
		return fmt.Errorf("build dispatch worker: %w", err)
	}

	subRegistry := config.NewSubscriptionRegistry(cfg.Subscriptions)
	registryClient, err := schema.NewRegistryClient(
		cfg.SchemaRegistry.Endpoint, cfg.SchemaRegistry.RequestTimeout, metrics,
	)
	if err != nil {
		return fmt.Errorf("build schema registry client: %w", err)
	}
	registryValidator, err := schema.NewRegistryValidator(registryClient, schema.RegistryValidatorConfig{
		GroupName:    cfg.SchemaRegistry.GroupName,
		CacheEnabled: cfg.SchemaRegistry.Cache.Enabled,
		CacheTTL:     cfg.SchemaRegistry.Cache.TTL,
		Observer:     metrics,
	})
	if err != nil {
		return fmt.Errorf("build schema registry validator: %w", err)
	}
	if err := registryValidator.Warm(ctx, subRegistry.EventTypes()); err != nil {
		return fmt.Errorf("warm schema registry cache: %w", err)
	}
	kafkaConsumer, err := consumer.New(consumer.Config{
		Brokers:        cfg.Kafka.Brokers,
		GroupID:        cfg.Dispatcher.Consumer.GroupID,
		SessionTimeout: cfg.Dispatcher.Consumer.SessionTimeout,
		EventTypes:     subRegistry.EventTypes(),
		Probe:          probe,
	}, subRegistry, pool, dispatchRepo, registryValidator.ForPath("consumer"), deadLetterRepo,
		consumer.Metrics{Consumed: metrics.DispatchConsumed},
		log)
	if err != nil {
		return fmt.Errorf("build kafka consumer: %w", err)
	}
	defer kafkaConsumer.Close()

	if err := metrics.RegisterKafkaLagGauge(kafkaConsumer); err != nil {
		return fmt.Errorf("register kafka lag gauge: %w", err)
	}

	metricsSrv := &nethttp.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Observability.Metrics.Port),
		Handler:           metricsMux(metrics),
		ReadHeaderTimeout: 2 * time.Second,
	}

	log.Info("gateway-worker starting",
		zap.Duration("relay_poll_interval", cfg.Relay.PollInterval),
		zap.Int("relay_batch_size", cfg.Relay.BatchSize),
		zap.Int("relay_max_retries", cfg.Relay.MaxRetries),
		zap.String("dispatch_group_id", cfg.Dispatcher.Consumer.GroupID),
		zap.Strings("dispatch_topics", subRegistry.EventTypes()),
	)

	errCh := make(chan error, 4)
	go func() { errCh <- worker.Run(ctx) }()
	go func() { errCh <- dispatchWorker.Run(ctx) }()
	go func() { errCh <- kafkaConsumer.Run(ctx) }()
	go kafkaConsumer.StartLagReporter(ctx, 5*time.Second)
	go func() { errCh <- metricsSrv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = metricsSrv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if errors.Is(err, context.Canceled) || errors.Is(err, nethttp.ErrServerClosed) {
			return nil
		}
		return err
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
