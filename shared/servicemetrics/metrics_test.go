package servicemetrics_test

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/victorvcruz/event-gateway/shared/servicemetrics"
)

func TestDispatchAndHTTPTelemetry(t *testing.T) {
	metrics := servicemetrics.New("test-service", nil, false)
	dispatch := metrics.WrapDispatch(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	mux := http.NewServeMux()
	mux.Handle("POST /dispatch", dispatch)
	handler := metrics.WrapHTTP(mux)

	req := httptest.NewRequest(http.MethodPost, "/dispatch", strings.NewReader(`{"event_type":"order.created"}`))
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	require.Equal(t, http.StatusConflict, res.Code)

	families, err := metrics.Registry.Gather()
	require.NoError(t, err)
	seen := map[string]bool{}
	for _, family := range families {
		seen[family.GetName()] = true
	}
	require.True(t, seen["business_dispatch_total"])
	require.False(t, seen["business_domain_operations_total"],
		"transport dispatch must not be counted as a domain operation")
	require.True(t, seen["business_http_request_duration_seconds"])
}

func TestOutboxCollectorReportsNaNWhenDatabaseIsUnavailable(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://invalid:invalid@127.0.0.1:1/invalid?sslmode=disable&connect_timeout=1")
	require.NoError(t, err)
	defer pool.Close()
	metrics := servicemetrics.New("test-service", pool, true)

	families, err := metrics.Registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "business_outbox_rows" {
			continue
		}
		require.Len(t, family.Metric, 3)
		for _, metric := range family.Metric {
			require.True(t, math.IsNaN(metric.GetGauge().GetValue()))
		}
		return
	}
	t.Fatal("business_outbox_rows was not gathered")
}
