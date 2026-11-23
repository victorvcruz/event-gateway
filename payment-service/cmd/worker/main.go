package main

import (
	"context"
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
	ctx, c := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer c()
	log, _ := logging.New(logging.Config{ServiceID: "payment-worker", Level: "info"})
	pool, err := postgres.NewPool(ctx, os.Getenv("DATABASE_DSN"))
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	gw, err := client.New(os.Getenv("GATEWAY_URL"))
	if err != nil {
		panic(err)
	}
	s, _ := serviceoutbox.New(gw)
	probe := failpoint.New("payment-worker", log)
	w, err := outbox.NewWorker(outbox.NewClient(pool, ""), s, outbox.Config{BatchSize: 100, PollInterval: 500 * time.Millisecond, MaxRetries: 6, BackoffSchedule: outbox.BackoffExponential(time.Second), Probe: probe}, log)
	if err != nil {
		panic(err)
	}
	_ = w.Run(ctx)
}
