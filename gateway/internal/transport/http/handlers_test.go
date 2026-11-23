package http_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/victorvcruz/event-gateway/gateway/internal/ingest"
	"github.com/victorvcruz/event-gateway/gateway/internal/observability"
	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
	transporthttp "github.com/victorvcruz/event-gateway/gateway/internal/transport/http"
	"github.com/victorvcruz/event-gateway/shared/failpoint"
)

type probeFunc func(context.Context, failpoint.Checkpoint)

func (f probeFunc) Hit(ctx context.Context, checkpoint failpoint.Checkpoint) { f(ctx, checkpoint) }

type stubIngest struct {
	res     ingest.Result
	outcome ingest.Outcome
	err     error

	// captured
	gotEnvelope ingest.Envelope
	gotRaw      json.RawMessage
}

type stubReadiness struct{ err error }

func (s stubReadiness) Ping(context.Context) error { return s.err }

func (s *stubIngest) Handle(_ context.Context, env ingest.Envelope, raw json.RawMessage) (ingest.Result, ingest.Outcome, error) {
	s.gotEnvelope = env
	s.gotRaw = raw
	return s.res, s.outcome, s.err
}

func body() string {
	return `{
	  "event_id": "` + uuid.NewString() + `",
	  "event_type": "order.created",
	  "correlation_id": "saga-1",
	  "timestamp": "2026-08-10T00:00:00Z",
	  "payload": {"n": 1}
	}`
}

func TestEventsHandler202(t *testing.T) {
	id := uuid.New()
	stub := &stubIngest{res: ingest.Result{EventID: id}, outcome: ingest.OutcomeAccepted}
	h := transporthttp.EventsHandler(transporthttp.Deps{Ingest: stub})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/events", strings.NewReader(body()))
	h.ServeHTTP(rec, req)

	require.Equal(t, 202, rec.Code)
	var got map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, id.String(), got["event_id"])
	assert.Equal(t, "order.created", stub.gotEnvelope.EventType)
}

func TestEventsHandlerHitsCheckpointOnlyAfterAcceptedCommit(t *testing.T) {
	id := uuid.New()
	stub := &stubIngest{res: ingest.Result{EventID: id}, outcome: ingest.OutcomeAccepted}
	var got failpoint.Checkpoint
	h := transporthttp.EventsHandler(transporthttp.Deps{
		Ingest: stub,
		Probe: probeFunc(func(_ context.Context, checkpoint failpoint.Checkpoint) {
			got = checkpoint
		}),
	})
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest("POST", "/events", strings.NewReader(body())))
	require.Equal(t, 202, recorder.Code)
	require.Equal(t, failpoint.GatewayHTTPAfterIngestCommit, got.Point)
	require.Equal(t, id.String(), got.EventID)
	require.Equal(t, "saga-1", got.CorrelationID)
}

func TestEventsHandler409(t *testing.T) {
	id := uuid.New()
	stub := &stubIngest{
		res:     ingest.Result{EventID: id, ExistingStatus: "SENT"},
		outcome: ingest.OutcomeDuplicate,
	}
	h := transporthttp.EventsHandler(transporthttp.Deps{Ingest: stub})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/events", strings.NewReader(body()))
	h.ServeHTTP(rec, req)

	require.Equal(t, 409, rec.Code)
	var got map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, id.String(), got["event_id"])
	assert.Equal(t, "SENT", got["existing_status"])
}

func TestEventsHandler422SchemaViolation(t *testing.T) {
	stub := &stubIngest{
		outcome: ingest.OutcomeSchemaViolation,
		err:     errors.New("does not validate"),
	}
	h := transporthttp.EventsHandler(transporthttp.Deps{Ingest: stub})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/events", strings.NewReader(body()))
	h.ServeHTTP(rec, req)

	require.Equal(t, 422, rec.Code)
	assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	var problem map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &problem))
	assert.Equal(t, "schema_violation", problem["code"])
	assert.Equal(t, "order.created", problem["event_type"])
	assert.Equal(t, "does not validate", problem["detail"])
}

func TestEventsHandler422SchemaNotFound(t *testing.T) {
	stub := &stubIngest{
		outcome: ingest.OutcomeSchemaNotFound,
		err:     schema.ErrSchemaNotFound,
	}
	h := transporthttp.EventsHandler(transporthttp.Deps{Ingest: stub})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/events", strings.NewReader(body()))
	h.ServeHTTP(rec, req)

	require.Equal(t, 422, rec.Code)
	assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	var problem map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &problem))
	assert.Equal(t, "schema_not_found", problem["code"])
}

func TestEventsHandlerUnknownEventTypesShareBoundedMetricLabel(t *testing.T) {
	metrics := observability.New()
	stub := &stubIngest{outcome: ingest.OutcomeSchemaNotFound, err: schema.ErrSchemaNotFound}
	h := transporthttp.EventsHandler(transporthttp.Deps{
		Ingest: stub, IngestLatency: metrics.IngestLatency, EventsReceived: metrics.EventsReceived,
	})
	for _, eventType := range []string{"attacker.one", "attacker.two", "attacker.three"} {
		raw := strings.Replace(body(), "order.created", eventType, 1)
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, httptest.NewRequest("POST", "/events", strings.NewReader(raw)))
		require.Equal(t, 422, recorder.Code)
	}

	families, err := metrics.Registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "gateway_events_received_total" {
			continue
		}
		require.Len(t, family.Metric, 1)
		for _, label := range family.Metric[0].Label {
			if label.GetName() == "event_type" {
				assert.Equal(t, "unknown", label.GetValue())
				return
			}
		}
	}
	t.Fatal("ingest event_type label not found")
}

func TestEventsHandler503RegistryUnavailable(t *testing.T) {
	stub := &stubIngest{
		outcome: ingest.OutcomeRegistryUnavailable,
		err:     schema.ErrRegistryUnavailable,
	}
	h := transporthttp.EventsHandler(transporthttp.Deps{Ingest: stub})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/events", strings.NewReader(body())))

	require.Equal(t, 503, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))
	assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	var problem map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &problem))
	assert.Equal(t, "schema_registry_unavailable", problem["code"])
	assert.NotContains(t, problem["detail"], "connection refused")
}

func TestEventsHandler500InternalFailure(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	stub := &stubIngest{outcome: ingest.OutcomeInternal, err: errors.New("database unavailable")}
	h := transporthttp.EventsHandler(transporthttp.Deps{Ingest: stub, Log: zap.New(core)})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/events", strings.NewReader(body()))
	h.ServeHTTP(rec, req)

	require.Equal(t, 500, rec.Code)
	assert.Equal(t, "internal server error", rec.Body.String())
	assert.NotContains(t, rec.Body.String(), "database unavailable")
	require.Equal(t, 1, logs.Len())
	assert.Equal(t, "database unavailable", logs.All()[0].ContextMap()["error"])
}

func TestEventsHandler400Malformed(t *testing.T) {
	stub := &stubIngest{}
	h := transporthttp.EventsHandler(transporthttp.Deps{Ingest: stub})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/events", strings.NewReader(`{not-json`))
	h.ServeHTTP(rec, req)

	require.Equal(t, 400, rec.Code)
}

func TestEventsHandlerMethodNotAllowed(t *testing.T) {
	h := transporthttp.EventsHandler(transporthttp.Deps{Ingest: &stubIngest{}})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/events", nil)
	h.ServeHTTP(rec, req)

	require.Equal(t, 405, rec.Code)
}

func TestHealthz(t *testing.T) {
	h := transporthttp.HealthzHandler(stubReadiness{})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/healthz", nil)
	h.ServeHTTP(rec, req)

	require.Equal(t, 200, rec.Code)
	var got map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, "ok", got["status"])
}

func TestHealthzReturns503WhenDatabaseIsUnavailable(t *testing.T) {
	h := transporthttp.HealthzHandler(stubReadiness{err: errors.New("database unavailable")})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/healthz", nil)
	h.ServeHTTP(rec, req)

	require.Equal(t, 503, rec.Code)
	var got map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, "unavailable", got["status"])
}
