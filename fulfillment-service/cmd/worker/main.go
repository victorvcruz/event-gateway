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
	c, x := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer x()
	l, _ := logging.New(logging.Config{ServiceID: "fulfillment-worker"})
	p, e := postgres.NewPool(c, os.Getenv("DATABASE_DSN"))
	if e != nil {
		panic(e)
	}
	g, e := client.New(os.Getenv("GATEWAY_URL"))
	if e != nil {
		panic(e)
	}
	s, _ := serviceoutbox.New(g)
	probe := failpoint.New("fulfillment-worker", l)
	w, e := outbox.NewWorker(outbox.NewClient(p, ""), s, outbox.Config{BatchSize: 100, PollInterval: 500 * time.Millisecond, MaxRetries: 6, BackoffSchedule: outbox.BackoffExponential(time.Second), Probe: probe}, l)
	if e != nil {
		panic(e)
	}
	_ = w.Run(c)
}
