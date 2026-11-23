// Package http tem os handlers do gateway-server: POST /events, GET /healthz
// e GET /metrics. Sem regra de negócio: só decodifica, chama o ingest.Service
// e serializa a resposta.
package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	nethttp "net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/gateway/internal/ingest"
	"github.com/victorvcruz/event-gateway/gateway/internal/observability"
	"github.com/victorvcruz/event-gateway/shared/failpoint"
)

type IngestService interface {
	Handle(ctx context.Context, env ingest.Envelope, raw json.RawMessage) (ingest.Result, ingest.Outcome, error)
}

type ReadinessChecker interface {
	Ping(context.Context) error
}

type Deps struct {
	Ingest         IngestService
	Registry       *prometheus.Registry
	IngestLatency  *prometheus.HistogramVec
	EventsReceived *prometheus.CounterVec
	Log            *zap.Logger
	Readiness      ReadinessChecker
	Probe          failpoint.Probe
}

// EventsHandler devolve o handler de POST /events.
func EventsHandler(d Deps) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if r.Method != nethttp.MethodPost {
			nethttp.Error(w, "method not allowed", nethttp.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()

		raw, err := readBody(r)
		if err != nil {
			writeText(w, nethttp.StatusBadRequest, err.Error())
			return
		}

		var env ingest.Envelope
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&env); err != nil {
			writeText(w, nethttp.StatusBadRequest, err.Error())
			return
		}

		start := time.Now()
		res, outcome, err := d.Ingest.Handle(r.Context(), env, raw)
		metricEventType := observability.EventTypeLabel(env.EventType)
		if d.IngestLatency != nil {
			d.IngestLatency.WithLabelValues(metricEventType).Observe(time.Since(start).Seconds())
		}

		latency := time.Since(start)
		log := d.Log
		if log == nil {
			log = zap.NewNop()
		}
		reqLog := log.With(
			zap.String("event_type", env.EventType),
			zap.String("event_id", res.EventID.String()),
			zap.String("correlation_id", env.CorrelationID),
			zap.Duration("latency", latency),
		)

		switch outcome {
		case ingest.OutcomeAccepted:
			if d.Probe != nil {
				d.Probe.Hit(r.Context(), failpoint.Checkpoint{
					Point:         failpoint.GatewayHTTPAfterIngestCommit,
					EventID:       res.EventID.String(),
					EventType:     env.EventType,
					CorrelationID: env.CorrelationID,
				})
			}
			if d.EventsReceived != nil {
				d.EventsReceived.WithLabelValues(metricEventType, "accepted").Inc()
			}
			reqLog.Info("event accepted", zap.Int("status", nethttp.StatusAccepted))
			writeJSON(w, nethttp.StatusAccepted, map[string]string{"event_id": res.EventID.String()})
		case ingest.OutcomeDuplicate:
			if d.EventsReceived != nil {
				d.EventsReceived.WithLabelValues(metricEventType, "conflict").Inc()
			}
			reqLog.Info("event duplicate",
				zap.Int("status", nethttp.StatusConflict),
				zap.String("existing_status", res.ExistingStatus))
			writeJSON(w, nethttp.StatusConflict, map[string]string{
				"event_id":        res.EventID.String(),
				"existing_status": res.ExistingStatus,
			})
		case ingest.OutcomeSchemaNotFound, ingest.OutcomeSchemaViolation:
			if d.EventsReceived != nil {
				d.EventsReceived.WithLabelValues(metricEventType, "invalid").Inc()
			}
			reqLog.Warn("event schema invalid",
				zap.Int("status", nethttp.StatusUnprocessableEntity),
				zap.Error(err))
			code := "schema_violation"
			title := "Event schema validation failed"
			if outcome == ingest.OutcomeSchemaNotFound {
				code = "schema_not_found"
				title = "Event schema was not found"
			}
			writeProblem(w, nethttp.StatusUnprocessableEntity, code, title, errMessage(err), env.EventType)
		case ingest.OutcomeRegistryUnavailable:
			if d.EventsReceived != nil {
				d.EventsReceived.WithLabelValues(metricEventType, "error").Inc()
			}
			reqLog.Error("schema registry unavailable", zap.Error(err))
			w.Header().Set("Retry-After", "1")
			writeProblem(w, nethttp.StatusServiceUnavailable, "schema_registry_unavailable",
				"Schema Registry is unavailable", "Schema validation is temporarily unavailable.", env.EventType)
		default:
			if d.EventsReceived != nil {
				d.EventsReceived.WithLabelValues(metricEventType, "error").Inc()
			}
			reqLog.Error("event ingest failed",
				zap.Int("status", nethttp.StatusInternalServerError),
				zap.Error(err))
			writeText(w, nethttp.StatusInternalServerError, "internal server error")
		}
	}
}

// HealthzHandler responde OK enquanto todas as dependências configuradas
// estiverem acessíveis. Em produção, são o PostgreSQL e o Schema Registry.
func HealthzHandler(checker ReadinessChecker) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		defer cancel()
		if checker == nil || checker.Ping(ctx) != nil {
			writeJSON(w, nethttp.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
		writeJSON(w, nethttp.StatusOK, map[string]string{"status": "ok"})
	}
}

// MetricsHandler expõe o registry do Prometheus.
func MetricsHandler(reg *prometheus.Registry) nethttp.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

const maxBodyBytes = 1 << 20

func readBody(r *nethttp.Request) (json.RawMessage, error) {
	body := nethttp.MaxBytesReader(nil, r.Body, maxBodyBytes)
	raw, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, errors.New("empty body")
	}
	return json.RawMessage(raw), nil
}

func writeJSON(w nethttp.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func writeText(w nethttp.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(body))
}

type problem struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Code      string `json:"code"`
	Detail    string `json:"detail"`
	EventType string `json:"event_type,omitempty"`
}

func writeProblem(w nethttp.ResponseWriter, status int, code, title, detail, eventType string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem{
		Type: "urn:event-gateway:error:" + code, Title: title, Status: status,
		Code: code, Detail: detail, EventType: eventType,
	})
}

func errMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
