package experimentcontrol

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/victorvcruz/event-gateway/gateway/pkg/client"
	"github.com/victorvcruz/event-gateway/shared/outbox"
)

type fakeStore struct {
	orders  []Order
	created bool
	err     error
}

type delayedSender struct{ release <-chan struct{} }

func (s delayedSender) Send(context.Context, outbox.Row) error {
	<-s.release
	return nil
}

func TestKafkaPublisherEnforcesCallerDeadlineWhenSenderIsStuck(t *testing.T) {
	release := make(chan struct{})
	publisher := NewKafkaPublisher(delayedSender{release: release})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := publisher.Publish(ctx, client.Event{
		EventID: uuid.New(), EventType: "order.created", CorrelationID: uuid.NewString(), Payload: json.RawMessage(`{}`),
	})
	close(release)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func (s *fakeStore) Create(_ context.Context, order Order) (Order, bool, error) {
	s.orders = append(s.orders, order)
	return order, s.created, s.err
}
func (s *fakeStore) Ping(context.Context) error { return nil }

type fakePublisher struct {
	events []client.Event
	err    error
}

func (p *fakePublisher) Publish(_ context.Context, event client.Event) error {
	p.events = append(p.events, event)
	return p.err
}

type fakeValidator struct {
	calls int
	err   error
}

func (v *fakeValidator) Validate(context.Context, string, json.RawMessage) error {
	v.calls++
	return v.err
}

func directRequest() *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/orders/direct", strings.NewReader(
		`{"run_id":"run-a","customer_id":"control","amount_cents":1000,"items":[{"sku":"SKU-001","quantity":1}]}`,
	))
	request.Header.Set("Idempotency-Key", uuid.NewString())
	return request
}

func TestDirectOrderCommitsBeforePublishFailure(t *testing.T) {
	store := &fakeStore{created: true}
	publisher := &fakePublisher{err: errors.New("broker unavailable")}
	validator := &fakeValidator{}
	app, err := New(store, publisher, validator, time.Second)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	app.Handler().ServeHTTP(recorder, directRequest())

	assert.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Len(t, store.orders, 1, "domain order remains committed")
	require.Len(t, publisher.events, 1)
	assert.Equal(t, store.orders[0].OrderID.String(), publisher.events[0].CorrelationID)
	assert.Equal(t, 1, validator.calls)
}

func TestProblemResponsesUseProblemJSONMediaType(t *testing.T) {
	app, err := New(&fakeStore{}, &fakePublisher{}, &fakeValidator{}, time.Second)
	require.NoError(t, err)
	request := directRequest()
	request.Header.Del("Idempotency-Key")
	recorder := httptest.NewRecorder()

	app.Handler().ServeHTTP(recorder, request)

	assert.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.Equal(t, "application/problem+json", recorder.Header().Get("Content-Type"))
}

func TestDirectOrderDuplicateDoesNotRepublish(t *testing.T) {
	store := &fakeStore{created: false}
	publisher := &fakePublisher{}
	app, err := New(store, publisher, &fakeValidator{}, time.Second)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	app.Handler().ServeHTTP(recorder, directRequest())

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Empty(t, publisher.events)
}

func TestBenchmarkSchemaUsesValidatorWhileBaselineDoesNot(t *testing.T) {
	validator := &fakeValidator{}
	app, err := New(&fakeStore{}, &fakePublisher{}, validator, time.Second)
	require.NoError(t, err)
	body := `{"event_id":"` + uuid.NewString() + `","event_type":"order.cancelled","correlation_id":"run","timestamp":"2026-08-28T00:00:00Z","payload":{"order_id":"` + uuid.NewString() + `","reason":"customer_request"}}`

	for _, path := range []string{"/benchmark/baseline", "/benchmark/schema"} {
		recorder := httptest.NewRecorder()
		app.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
		require.Equal(t, http.StatusAccepted, recorder.Code)
	}
	assert.Equal(t, 1, validator.calls)
}
