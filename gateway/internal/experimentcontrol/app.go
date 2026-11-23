package experimentcontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
	"github.com/victorvcruz/event-gateway/gateway/pkg/client"
	"github.com/victorvcruz/event-gateway/shared/contracts"
)

const maxBodyBytes = 1 << 20

type EventPublisher interface {
	Publish(context.Context, client.Event) error
}

type Order struct {
	OrderID        uuid.UUID
	EventID        uuid.UUID
	IdempotencyKey string
	RunID          string
	CustomerID     string
	AmountCents    int64
	Items          []contracts.Item
	EventTimestamp time.Time
}

type OrderRequest struct {
	RunID       string           `json:"run_id"`
	CustomerID  string           `json:"customer_id"`
	AmountCents int64            `json:"amount_cents"`
	Items       []contracts.Item `json:"items"`
}

type App struct {
	store          Store
	publisher      EventPublisher
	validator      schema.Validator
	publishTimeout time.Duration
	registry       *prometheus.Registry
	duration       *prometheus.HistogramVec
}

func New(store Store, publisher EventPublisher, validator schema.Validator, publishTimeout time.Duration) (*App, error) {
	if store == nil || publisher == nil || validator == nil {
		return nil, errors.New("experiment control dependencies are required")
	}
	if publishTimeout <= 0 {
		return nil, errors.New("publish timeout must be positive")
	}
	registry := prometheus.NewRegistry()
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "experiment_control_http_duration_seconds",
		Help:    "Request duration for experimental control variants.",
		Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"route", "status"})
	registry.MustRegister(duration)
	return &App{store: store, publisher: publisher, validator: validator,
		publishTimeout: publishTimeout, registry: registry, duration: duration}, nil
}

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders/direct", a.directOrder)
	mux.HandleFunc("POST /benchmark/baseline", a.benchmark(false))
	mux.HandleFunc("POST /benchmark/schema", a.benchmark(true))
	mux.HandleFunc("GET /healthz", a.health)
	mux.Handle("GET /metrics", promhttp.HandlerFor(a.registry, promhttp.HandlerOpts{}))
	return a.observe(mux)
}

func (a *App) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		a.duration.WithLabelValues(route, fmt.Sprint(rw.status)).Observe(time.Since(start).Seconds())
	})
}

func (a *App) directOrder(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" || len(key) > 255 {
		writeProblem(w, http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key is required")
		return
	}
	var request OrderRequest
	raw, err := decodeStrict(r.Body, &request)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	_ = raw
	payload := contracts.OrderCreated{
		OrderID: uuid.NewString(), CustomerID: request.CustomerID,
		AmountCents: request.AmountCents, Items: request.Items,
	}
	if strings.TrimSpace(request.RunID) == "" || len(request.RunID) > 255 {
		writeProblem(w, http.StatusBadRequest, "invalid_run_id", "run_id is required")
		return
	}
	if err := contracts.ValidateOrderCreated(payload); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	now := time.Now().UTC()
	eventID := uuid.New()
	payloadJSON, _ := json.Marshal(payload)
	event := client.Event{
		EventID: eventID, EventType: "order.created", CorrelationID: payload.OrderID,
		Payload: payloadJSON, OccurredAt: now,
	}
	envelope, _ := json.Marshal(event)
	if err := a.validator.Validate(r.Context(), event.EventType, envelope); err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "schema_validation_failed", err.Error())
		return
	}
	order := Order{
		OrderID: uuid.MustParse(payload.OrderID), EventID: eventID, IdempotencyKey: key,
		RunID: request.RunID, CustomerID: request.CustomerID, AmountCents: request.AmountCents,
		Items: request.Items, EventTimestamp: now,
	}
	stored, created, err := a.store.Create(r.Context(), order)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "control_store_failed", "internal server error")
		return
	}
	if !created {
		writeJSON(w, http.StatusOK, map[string]any{
			"order_id": stored.OrderID, "event_id": stored.EventID, "publish_outcome": "duplicate",
		})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.publishTimeout)
	defer cancel()
	if err := a.publisher.Publish(ctx, event); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"order_id": stored.OrderID, "event_id": stored.EventID,
			"publish_outcome": "failed", "code": "kafka_publish_failed",
		})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"order_id": stored.OrderID, "event_id": stored.EventID, "publish_outcome": "ack",
	})
}

func (a *App) benchmark(validate bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var event client.Event
		raw, err := decodeStrict(r.Body, &event)
		if err != nil || event.EventID == uuid.Nil || strings.TrimSpace(event.EventType) == "" ||
			strings.TrimSpace(event.CorrelationID) == "" || event.OccurredAt.IsZero() || len(event.Payload) == 0 {
			writeProblem(w, http.StatusBadRequest, "invalid_envelope", "valid event envelope is required")
			return
		}
		if validate {
			if err := a.validator.Validate(r.Context(), event.EventType, raw); err != nil {
				status := http.StatusUnprocessableEntity
				if errors.Is(err, schema.ErrRegistryUnavailable) {
					status = http.StatusServiceUnavailable
				}
				writeProblem(w, status, "schema_validation_failed", err.Error())
				return
			}
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"event_id": event.EventID.String()})
	}
}

func (a *App) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := a.store.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func decodeStrict(body io.Reader, dst any) (json.RawMessage, error) {
	raw, err := io.ReadAll(io.LimitReader(body, maxBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || len(raw) > maxBodyBytes {
		return nil, errors.New("request body is empty or too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("trailing JSON")
	}
	return raw, nil
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	writeJSONWithType(w, status, "application/json", body)
}

func writeJSONWithType(w http.ResponseWriter, status int, contentType string, body any) {
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeProblem(w http.ResponseWriter, status int, code, detail string) {
	writeJSONWithType(w, status, "application/problem+json", map[string]any{
		"type": "urn:event-gateway:experiment:" + code, "title": http.StatusText(status),
		"status": status, "code": code, "detail": detail,
	})
}
