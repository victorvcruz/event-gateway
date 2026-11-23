package http

import (
	nethttp "net/http"
)

// NewRouter monta o *http.ServeMux com as três rotas do gateway-server.
func NewRouter(d Deps) *nethttp.ServeMux {
	mux := nethttp.NewServeMux()
	mux.Handle("POST /events", EventsHandler(d))
	mux.Handle("GET /healthz", HealthzHandler(d.Readiness))
	if d.Registry != nil {
		mux.Handle("GET /metrics", MetricsHandler(d.Registry))
	}
	return mux
}
