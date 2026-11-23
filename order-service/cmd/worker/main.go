package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/victorvcruz/event-gateway/gateway/pkg/client"
	"github.com/victorvcruz/event-gateway/gateway/pkg/serviceoutbox"
	"github.com/victorvcruz/event-gateway/shared/failpoint"
	"github.com/victorvcruz/event-gateway/shared/logging"
	"github.com/victorvcruz/event-gateway/shared/outbox"
	"github.com/victorvcruz/event-gateway/shared/postgres"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	log, _ := logging.New(logging.Config{ServiceID: "order-worker", Level: "info"})
	defer func() { _ = log.Sync() }()
	pool, err := postgres.NewPool(ctx, os.Getenv("DATABASE_DSN"))
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	gw, err := client.New(os.Getenv("GATEWAY_URL"))
	if err != nil {
		panic(err)
	}
	sender, _ := serviceoutbox.New(gw)
	probe := failpoint.New("order-worker", log)
	// Padrão: 6 tentativas com espera exponencial a partir de 1 s (≈31 s no
	// total). OUTBOX_MAX_ATTEMPTS e OUTBOX_MAX_BACKOFF só são usadas na
	// execução suplementar do Experimento C.
	maxAttempts, backoff, err := outbox.RetryPolicyFromEnv(6, time.Second)
	if err != nil {
		panic(err)
	}
	worker, err := outbox.NewWorker(outbox.NewClient(pool, ""), sender, outbox.Config{BatchSize: 100, PollInterval: 500 * time.Millisecond, MaxRetries: maxAttempts, BackoffSchedule: backoff, Probe: probe}, log)
	if err != nil {
		panic(err)
	}
	if err = worker.Run(ctx); err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, err)
	}
}
