package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/victorvcruz/event-gateway/fulfillment-service/internal/app"
	"github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
	"github.com/victorvcruz/event-gateway/shared/failpoint"
	"github.com/victorvcruz/event-gateway/shared/healthz"
	"github.com/victorvcruz/event-gateway/shared/logging"
	"github.com/victorvcruz/event-gateway/shared/postgres"
	"github.com/victorvcruz/event-gateway/shared/servicemetrics"
)

func main() {
	ctx, c := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer c()
	l, _ := logging.New(logging.Config{ServiceID: "fulfillment-service", Level: "info"})
	p, e := postgres.NewPool(ctx, os.Getenv("DATABASE_DSN"))
	if e != nil {
		panic(e)
	}
	defer p.Close()
	a := app.New(p, l)
	probe := failpoint.New("fulfillment-service", l)
	metrics := servicemetrics.New("fulfillment-service", p, true)
	m := http.NewServeMux()
	dispatch := a.Dispatcher(dispatcher.WithAfterHandle(func(ctx context.Context, evt dispatcher.Event) {
		probe.Hit(ctx, failpoint.Checkpoint{Point: failpoint.DispatchAfterHandlerCommit, EventID: evt.EventID.String(), EventType: evt.EventType, CorrelationID: evt.CorrelationID})
	}))
	m.Handle("POST /dispatch", failpoint.Middleware(probe, dispatcher.Authenticate(os.Getenv("DISPATCH_TOKEN"), metrics.WrapDispatch(dispatch))))
	m.Handle("GET /healthz", healthz.Handler(p))
	m.Handle("GET /metrics", metrics.Handler())
	s := &http.Server{Addr: ":" + env("SERVICE_PORT", "8084"), Handler: metrics.WrapHTTP(m), ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); _ = s.Shutdown(context.Background()) }()
	if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
