// Package observability define as métricas Prometheus do gateway. O gauge de
// PENDING consulta o pool a cada scrape.
package observability

import (
	"context"
	"math"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type Metrics struct {
	Registry *prometheus.Registry

	EventsReceived        *prometheus.CounterVec
	EventsPublished       *prometheus.CounterVec
	IngestLatency         *prometheus.HistogramVec
	RelayPublishLatency   *prometheus.HistogramVec
	DeadLetterTotal       *prometheus.CounterVec
	pendingGaugeCollector *pendingGauge

	DispatchConsumed    *prometheus.CounterVec
	DispatchAttempted   *prometheus.CounterVec
	DispatchLatency     *prometheus.HistogramVec
	DispatchDeadLetter  *prometheus.CounterVec
	dispatchPendingCol  *dispatchPendingGauge
	dispatchKafkaLagCol *kafkaLagGauge

	SchemaValidations       *prometheus.CounterVec
	SchemaValidationLatency *prometheus.HistogramVec
	SchemaCache             *prometheus.CounterVec
	SchemaRegistryRequests  *prometheus.CounterVec
	SchemaRegistryLatency   *prometheus.HistogramVec
}

var knownEventTypeLabels = map[string]struct{}{
	"order.created":     {},
	"payment.processed": {},
	"payment.failed":    {},
	"payment.refunded":  {},
	"stock.reserved":    {},
	"stock.failed":      {},
	"stock.released":    {},
	"order.fulfilled":   {},
	"order.cancelled":   {},
}

// EventTypeLabel limita a cardinalidade do label, já que o event_type vem de
// envelopes não confiáveis. Tipos válidos mantêm o próprio nome; o resto
// vira uma série só.
func EventTypeLabel(eventType string) string {
	if _, ok := knownEventTypeLabels[eventType]; ok {
		return eventType
	}
	return "unknown"
}

func New() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		Registry: reg,
		EventsReceived: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gateway_events_received_total",
				Help: "POST /events por tipo e outcome (accepted|conflict|invalid|error).",
			},
			[]string{"event_type", "outcome"},
		),
		EventsPublished: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gateway_events_published_total",
				Help: "Tentativas do Relay por tipo e outcome (ack|retry|dead).",
			},
			[]string{"event_type", "outcome"},
		),
		IngestLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gateway_ingest_latency_seconds",
				Help:    "Latência do POST /events (Experimento B).",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"event_type"},
		),
		RelayPublishLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gateway_relay_publish_latency_seconds",
				Help:    "Latência de INSERT PENDING até UPDATE SENT.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"event_type"},
		),
		DeadLetterTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gateway_dead_letter_total",
				Help: "Movimentos para dead_letter por tipo.",
			},
			[]string{"event_type"},
		),
		DispatchConsumed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gateway_dispatch_consumed_total",
				Help: "Mensagens Kafka processadas pelo Consumer (outcome=persisted|duplicate|malformed|no_subscribers).",
			},
			[]string{"event_type", "outcome"},
		),
		DispatchAttempted: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gateway_dispatch_attempted_total",
				Help: "Tentativas HTTP de POST /dispatch por subscriber (outcome=ack|retry|dead).",
			},
			[]string{"event_type", "service_id", "outcome"},
		),
		DispatchLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gateway_dispatch_latency_seconds",
				Help:    "Latência de INSERT PENDING até UPDATE SENT no dispatch (Experimento B).",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"event_type", "service_id"},
		),
		DispatchDeadLetter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gateway_dispatch_dead_letter_total",
				Help: "Movimentos para dead_letter no lado do dispatch por subscriber.",
			},
			[]string{"event_type", "service_id"},
		),
		SchemaValidations: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "gateway_schema_validations_total", Help: "Validações de schema por caminho, tipo e resultado."},
			[]string{"path", "event_type", "outcome"},
		),
		SchemaValidationLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{Name: "gateway_schema_validation_latency_seconds", Help: "Latência da validação centralizada de schema.", Buckets: prometheus.DefBuckets},
			[]string{"path", "event_type"},
		),
		SchemaCache: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "gateway_schema_cache_total", Help: "Acessos ao cache de schemas por resultado."},
			[]string{"result"},
		),
		SchemaRegistryRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "gateway_schema_registry_requests_total", Help: "Requisições ao Schema Registry por operação e resultado."},
			[]string{"operation", "outcome"},
		),
		SchemaRegistryLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{Name: "gateway_schema_registry_request_latency_seconds", Help: "Latência das requisições ao Schema Registry.", Buckets: prometheus.DefBuckets},
			[]string{"operation"},
		),
	}

	reg.MustRegister(m.EventsReceived, m.EventsPublished, m.IngestLatency,
		m.RelayPublishLatency, m.DeadLetterTotal,
		m.DispatchConsumed, m.DispatchAttempted, m.DispatchLatency, m.DispatchDeadLetter,
		m.SchemaValidations, m.SchemaValidationLatency, m.SchemaCache,
		m.SchemaRegistryRequests, m.SchemaRegistryLatency)
	return m
}

func (m *Metrics) ObserveRegistryRequest(operation, outcome string, elapsed time.Duration) {
	m.SchemaRegistryRequests.WithLabelValues(operation, outcome).Inc()
	m.SchemaRegistryLatency.WithLabelValues(operation).Observe(elapsed.Seconds())
}

func (m *Metrics) ObserveSchemaCache(result string) {
	m.SchemaCache.WithLabelValues(result).Inc()
}

func (m *Metrics) ObserveSchemaValidation(path, eventType, outcome string, elapsed time.Duration) {
	eventType = EventTypeLabel(eventType)
	m.SchemaValidations.WithLabelValues(path, eventType, outcome).Inc()
	m.SchemaValidationLatency.WithLabelValues(path, eventType).Observe(elapsed.Seconds())
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

// RegisterPendingGauge registra gateway_outbox_pending_gauge, que conta as
// linhas PENDING do pool a cada scrape. Em produção, chame uma vez só.
func (m *Metrics) RegisterPendingGauge(pool *pgxpool.Pool, log *zap.Logger) error {
	c := newPendingGauge(pool, log)
	if err := m.Registry.Register(c); err != nil {
		return err
	}
	m.pendingGaugeCollector = c
	return nil
}

// pendingGauge é um prometheus.Collector que roda um COUNT leve a cada scrape.
// A query tem timeout de 250ms e, se falhar, devolve NaN para não quebrar o
// scrape.
type pendingGauge struct {
	pool *pgxpool.Pool
	log  *zap.Logger
	desc *prometheus.Desc
}

func newPendingGauge(pool *pgxpool.Pool, log *zap.Logger) *pendingGauge {
	return &pendingGauge{
		pool: pool,
		log:  log,
		desc: prometheus.NewDesc(
			"gateway_outbox_pending_gauge",
			"Número atual de linhas PENDING em gateway_outbox.",
			nil, nil,
		),
	}
}

func (p *pendingGauge) Describe(ch chan<- *prometheus.Desc) { ch <- p.desc }

func (p *pendingGauge) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	var n int64
	err := p.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE status='PENDING'`).Scan(&n)
	if err != nil {
		if p.log != nil {
			p.log.Warn("pending gauge scrape failed", zap.Error(err))
		}
		ch <- prometheus.MustNewConstMetric(p.desc, prometheus.GaugeValue, math.NaN())
		return
	}
	ch <- prometheus.MustNewConstMetric(p.desc, prometheus.GaugeValue, float64(n))
}
