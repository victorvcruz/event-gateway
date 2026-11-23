package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
	"github.com/victorvcruz/event-gateway/order-service/internal/app"
	"github.com/victorvcruz/event-gateway/shared/failpoint"
	"github.com/victorvcruz/event-gateway/shared/healthz"
	"github.com/victorvcruz/event-gateway/shared/logging"
	"github.com/victorvcruz/event-gateway/shared/postgres"
	"github.com/victorvcruz/event-gateway/shared/servicemetrics"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	log, err := logging.New(logging.Config{ServiceID: "order-service", Level: env("LOG_LEVEL", "info")})
	if err != nil {
		return err
	}
	defer func() { _ = log.Sync() }()
	pool, err := postgres.NewPool(ctx, mustEnv("DATABASE_DSN"))
	if err != nil {
		return err
	}
	defer pool.Close()
	metrics := servicemetrics.New("order-service", pool, true)
	probe := failpoint.New("order-service", log)
	a := app.New(pool, log, metrics.Domain)
	mux := http.NewServeMux()
	mux.Handle("/orders", failpoint.Middleware(probe, a.OrdersHandler()))
	mux.Handle("/orders/", failpoint.Middleware(probe, a.OrdersHandler()))
	dispatch := a.Dispatcher(dispatcher.WithAfterHandle(func(ctx context.Context, evt dispatcher.Event) {
		probe.Hit(ctx, failpoint.Checkpoint{Point: failpoint.DispatchAfterHandlerCommit, EventID: evt.EventID.String(), EventType: evt.EventType, CorrelationID: evt.CorrelationID})
	}))
	mux.Handle("POST /dispatch", failpoint.Middleware(probe, dispatcher.Authenticate(os.Getenv("DISPATCH_TOKEN"), metrics.WrapDispatch(dispatch))))
	mux.Handle("GET /healthz", healthz.Handler(pool))
	srv := &http.Server{Addr: ":" + env("SERVICE_PORT", "8081"), Handler: metrics.WrapHTTP(mux), ReadHeaderTimeout: 5 * time.Second}
	metricsServer := &http.Server{Addr: ":" + env("METRICS_PORT", "9090"), Handler: metrics.Handler(), ReadHeaderTimeout: 2 * time.Second}
	errs := make(chan error, 2)
	go func() { errs <- srv.ListenAndServe() }()
	go func() { errs <- metricsServer.ListenAndServe() }()
	select {
	case <-ctx.Done():
		c, x := context.WithTimeout(context.Background(), 5*time.Second)
		defer x()
		_ = metricsServer.Shutdown(c)
		return srv.Shutdown(c)
	case err := <-errs:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		panic(k + " is required")
	}
	return v
}

var _ = strconv.Itoa
