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
	"github.com/victorvcruz/event-gateway/payment-service/internal/app"
	"github.com/victorvcruz/event-gateway/shared/failpoint"
	"github.com/victorvcruz/event-gateway/shared/healthz"
	"github.com/victorvcruz/event-gateway/shared/logging"
	"github.com/victorvcruz/event-gateway/shared/postgres"
	"github.com/victorvcruz/event-gateway/shared/servicemetrics"
)

func main() {
	ctx, c := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer c()
	log, _ := logging.New(logging.Config{ServiceID: "payment-service", Level: "info"})
	defer func() { _ = log.Sync() }()
	pool, err := postgres.NewPool(ctx, os.Getenv("DATABASE_DSN"))
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	limit, err := parseApprovalLimit(env("PAYMENT_APPROVAL_LIMIT_CENTS", "100000"))
	if err != nil {
		panic(err)
	}
	a := app.New(pool, limit, log)
	probe := failpoint.New("payment-service", log)
	metrics := servicemetrics.New("payment-service", pool, true)
	mux := http.NewServeMux()
	dispatch := a.Dispatcher(dispatcher.WithAfterHandle(func(ctx context.Context, evt dispatcher.Event) {
		probe.Hit(ctx, failpoint.Checkpoint{Point: failpoint.DispatchAfterHandlerCommit, EventID: evt.EventID.String(), EventType: evt.EventType, CorrelationID: evt.CorrelationID})
	}))
	mux.Handle("POST /dispatch", failpoint.Middleware(probe, dispatcher.Authenticate(os.Getenv("DISPATCH_TOKEN"), metrics.WrapDispatch(dispatch))))
	mux.Handle("GET /healthz", healthz.Handler(pool))
	mux.Handle("GET /metrics", metrics.Handler())
	srv := &http.Server{Addr: ":" + env("SERVICE_PORT", "8082"), Handler: metrics.WrapHTTP(mux), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		x, y := context.WithTimeout(context.Background(), 5*time.Second)
		defer y()
		_ = srv.Shutdown(x)
	}()
	if err = srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}

func parseApprovalLimit(raw string) (int64, error) {
	limit, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || limit < 1 {
		return 0, fmt.Errorf("invalid PAYMENT_APPROVAL_LIMIT_CENTS %q: must be a positive integer", raw)
	}
	return limit, nil
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
