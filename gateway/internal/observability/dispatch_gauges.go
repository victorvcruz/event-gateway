package observability

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// RegisterDispatchPendingGauge registra gateway_dispatch_pending_gauge, que
// conta as linhas PENDING de dispatch_outbox a cada scrape. A consulta tem
// timeout de 250ms, como o gauge de pendentes do outbox.
func (m *Metrics) RegisterDispatchPendingGauge(pool *pgxpool.Pool, log *zap.Logger) error {
	c := newDispatchPendingGauge(pool, log)
	if err := m.Registry.Register(c); err != nil {
		return err
	}
	m.dispatchPendingCol = c
	return nil
}

type dispatchPendingGauge struct {
	pool *pgxpool.Pool
	log  *zap.Logger
	desc *prometheus.Desc
}

func newDispatchPendingGauge(pool *pgxpool.Pool, log *zap.Logger) *dispatchPendingGauge {
	return &dispatchPendingGauge{
		pool: pool,
		log:  log,
		desc: prometheus.NewDesc(
			"gateway_dispatch_pending_gauge",
			"Número atual de linhas PENDING em dispatch_outbox.",
			nil, nil,
		),
	}
}

func (g *dispatchPendingGauge) Describe(ch chan<- *prometheus.Desc) { ch <- g.desc }

func (g *dispatchPendingGauge) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	var n int64
	err := g.pool.QueryRow(ctx, `SELECT count(*) FROM dispatch_outbox WHERE status='PENDING'`).Scan(&n)
	if err != nil {
		if g.log != nil {
			g.log.Warn("dispatch pending gauge scrape failed", zap.Error(err))
		}
		ch <- prometheus.MustNewConstMetric(g.desc, prometheus.GaugeValue, math.NaN())
		return
	}
	ch <- prometheus.MustNewConstMetric(g.desc, prometheus.GaugeValue, float64(n))
}

// KafkaLagReporter é implementado pelo Consumer e expõe o lag atual por
// (topic, partition). O collector só lê o valor em memória a cada scrape.
type KafkaLagReporter interface {
	Snapshot() []KafkaLag
}

type KafkaLag struct {
	Topic     string
	Partition int32
	Lag       int64
}

// RegisterKafkaLagGauge registra gateway_dispatch_kafka_lag_gauge a partir do
// reporter informado. Chame depois de criar o Consumer.
func (m *Metrics) RegisterKafkaLagGauge(reporter KafkaLagReporter) error {
	c := newKafkaLagGauge(reporter)
	if err := m.Registry.Register(c); err != nil {
		return err
	}
	m.dispatchKafkaLagCol = c
	return nil
}

type kafkaLagGauge struct {
	mu       sync.RWMutex
	reporter KafkaLagReporter
	desc     *prometheus.Desc
}

func newKafkaLagGauge(r KafkaLagReporter) *kafkaLagGauge {
	return &kafkaLagGauge{
		reporter: r,
		desc: prometheus.NewDesc(
			"gateway_dispatch_kafka_lag_gauge",
			"Lag do consumer group do dispatcher por (topic, partition).",
			[]string{"topic", "partition"}, nil,
		),
	}
}

func (g *kafkaLagGauge) Describe(ch chan<- *prometheus.Desc) { ch <- g.desc }

func (g *kafkaLagGauge) Collect(ch chan<- prometheus.Metric) {
	g.mu.RLock()
	r := g.reporter
	g.mu.RUnlock()
	if r == nil {
		return
	}
	for _, l := range r.Snapshot() {
		ch <- prometheus.MustNewConstMetric(
			g.desc, prometheus.GaugeValue, float64(l.Lag),
			l.Topic, itoa32(l.Partition),
		)
	}
}

func itoa32(n int32) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := make([]byte, 0, 12)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}
