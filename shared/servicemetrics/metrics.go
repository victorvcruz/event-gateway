// Package servicemetrics tem as métricas Prometheus comuns aos serviços de
// negócio que só expõem HTTP.
package servicemetrics

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	ServiceID string
	Registry  *prometheus.Registry
	Dispatch  *prometheus.CounterVec
	Domain    *prometheus.CounterVec
	HTTP      *prometheus.HistogramVec
}

func New(serviceID string, pool *pgxpool.Pool, observeOutbox bool) *Metrics {
	registry := prometheus.NewRegistry()
	m := &Metrics{
		ServiceID: serviceID,
		Registry:  registry,
		Dispatch: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "business_dispatch_total",
			Help: "Dispatches handled by business services.",
		}, []string{"service_id", "event_type", "outcome"}),
		Domain: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "business_domain_operations_total",
			Help: "Business domain operations.",
		}, []string{"service_id", "operation", "outcome"}),
		HTTP: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "business_http_request_duration_seconds",
			Help:    "Business-service HTTP request duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"service_id", "method", "route", "status"}),
	}
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), m.Dispatch, m.Domain, m.HTTP)
	if observeOutbox && pool != nil {
		registry.MustRegister(&outboxCollector{pool: pool, serviceID: serviceID, desc: prometheus.NewDesc(
			"business_outbox_rows", "Current business outbox rows by status.", []string{"service_id", "status"}, nil,
		)})
	}
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

func (m *Metrics) WrapHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		m.HTTP.WithLabelValues(m.ServiceID, r.Method, route, strconv.Itoa(rw.status)).Observe(time.Since(start).Seconds())
	})
}

func (m *Metrics) WrapDispatch(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
		if err != nil || len(body) > 1<<20 {
			http.Error(w, "invalid dispatch body", http.StatusBadRequest)
			m.observeDispatch("unknown", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		var envelope struct {
			EventType string `json:"event_type"`
		}
		_ = json.Unmarshal(body, &envelope)
		if envelope.EventType == "" {
			envelope.EventType = "unknown"
		}
		rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		m.observeDispatch(envelope.EventType, rw.status)
	})
}

func (m *Metrics) observeDispatch(eventType string, status int) {
	outcome := "success"
	switch {
	case status == http.StatusConflict:
		outcome = "duplicate"
	case status >= 500:
		outcome = "retry"
	case status >= 400:
		outcome = "invalid"
	}
	m.Dispatch.WithLabelValues(m.ServiceID, eventType, outcome).Inc()
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

type outboxCollector struct {
	pool      *pgxpool.Pool
	serviceID string
	desc      *prometheus.Desc
}

func (c *outboxCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *outboxCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	counts := map[string]float64{"PENDING": 0, "SENT": 0, "DEAD": 0}
	rows, err := c.pool.Query(ctx, `SELECT status, count(*) FROM outbox GROUP BY status`)
	if err != nil {
		c.emitUnavailable(ch)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count float64
		if err := rows.Scan(&status, &count); err != nil {
			c.emitUnavailable(ch)
			return
		}
		counts[status] = count
	}
	if rows.Err() != nil {
		c.emitUnavailable(ch)
		return
	}
	for _, status := range []string{"PENDING", "SENT", "DEAD"} {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, counts[status], c.serviceID, status)
	}
}

func (c *outboxCollector) emitUnavailable(ch chan<- prometheus.Metric) {
	for _, status := range []string{"PENDING", "SENT", "DEAD"} {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, math.NaN(), c.serviceID, status)
	}
}
