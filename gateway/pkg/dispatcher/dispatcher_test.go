package dispatcher_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/victorvcruz/event-gateway/gateway/pkg/client"
	"github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
)

func makeEnvelope(t *testing.T, eventType string) []byte {
	t.Helper()
	body, err := json.Marshal(client.Event{
		EventID:       uuid.New(),
		EventType:     eventType,
		CorrelationID: "saga-dispatch",
		Payload:       json.RawMessage(`{"order_id":"x"}`),
		OccurredAt:    time.Now().UTC(),
	})
	require.NoError(t, err)
	return body
}

func postToDispatcher(t *testing.T, d *dispatcher.Dispatcher, body []byte, headers map[string]string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/dispatch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res := httptest.NewRecorder()
	d.ServeHTTP(res, req)
	return res.Result()
}

// (a) envelope válido + handler retornando nil → 200
func TestDispatcherRegisteredHandlerSuccessReturns200(t *testing.T) {
	d := dispatcher.New()
	called := 0
	d.Handle("order.created", func(_ context.Context, _ dispatcher.Event) error {
		called++
		return nil
	})

	resp := postToDispatcher(t, d, makeEnvelope(t, "order.created"), nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 1, called)
}

func TestDispatcherRunsAfterHandleHookAfterSuccessfulHandler(t *testing.T) {
	var handled bool
	var got dispatcher.Event
	d := dispatcher.New(dispatcher.WithAfterHandle(func(ctx context.Context, evt dispatcher.Event) {
		require.True(t, handled, "hook must run after the domain handler")
		require.Equal(t, "saga-dispatch", dispatcher.CorrelationIDFromContext(ctx))
		got = evt
	}))
	d.Handle("order.created", func(_ context.Context, _ dispatcher.Event) error {
		handled = true
		return nil
	})

	resp := postToDispatcher(t, d, makeEnvelope(t, "order.created"), nil)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "order.created", got.EventType)
	require.Equal(t, "saga-dispatch", got.CorrelationID)
	require.NotEqual(t, uuid.Nil, got.EventID)
}

func TestDispatcherDoesNotRunAfterHandleHookOnHandlerError(t *testing.T) {
	d := dispatcher.New(dispatcher.WithAfterHandle(func(context.Context, dispatcher.Event) {
		t.Fatal("failed handlers must not run the success hook")
	}))
	d.Handle("order.created", func(_ context.Context, _ dispatcher.Event) error { return errors.New("db down") })
	resp := postToDispatcher(t, d, makeEnvelope(t, "order.created"), nil)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// (b) handler retornando ErrDuplicate → 409
func TestDispatcherHandlerErrDuplicateReturns409(t *testing.T) {
	d := dispatcher.New()
	d.Handle("order.created", func(_ context.Context, _ dispatcher.Event) error {
		return dispatcher.ErrDuplicate
	})

	resp := postToDispatcher(t, d, makeEnvelope(t, "order.created"), nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

// (c) handler retornando erro wrapping ErrDuplicate → 409
func TestDispatcherHandlerWrappedErrDuplicateReturns409(t *testing.T) {
	d := dispatcher.New()
	d.Handle("order.created", func(_ context.Context, _ dispatcher.Event) error {
		return fmt.Errorf("payment already exists: %w", dispatcher.ErrDuplicate)
	})

	resp := postToDispatcher(t, d, makeEnvelope(t, "order.created"), nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

// (d) handler retornando erro genérico → 500
func TestDispatcherHandlerGenericErrorReturns500(t *testing.T) {
	d := dispatcher.New()
	d.Handle("order.created", func(_ context.Context, _ dispatcher.Event) error {
		return errors.New("db down")
	})

	resp := postToDispatcher(t, d, makeEnvelope(t, "order.created"), nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// (e) handler retornando ErrBadRequest → 400
func TestDispatcherHandlerErrBadRequestReturns400(t *testing.T) {
	d := dispatcher.New()
	d.Handle("order.created", func(_ context.Context, _ dispatcher.Event) error {
		return fmt.Errorf("cannot process: %w", dispatcher.ErrBadRequest)
	})

	resp := postToDispatcher(t, d, makeEnvelope(t, "order.created"), nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// (f) envelope com decode broken → 400
func TestDispatcherMalformedEnvelopeReturns400(t *testing.T) {
	d := dispatcher.New()
	d.Handle("order.created", func(_ context.Context, _ dispatcher.Event) error {
		t.Fatal("handler não deveria ser chamado com envelope quebrado")
		return nil
	})

	resp := postToDispatcher(t, d, []byte("not json at all"), nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestDispatcherRejectsIncompleteEnvelopeBeforeHandler(t *testing.T) {
	d := dispatcher.New()
	d.Handle("order.created", func(_ context.Context, _ dispatcher.Event) error {
		t.Fatal("handler must not receive an incomplete envelope")
		return nil
	})

	resp := postToDispatcher(t, d, []byte(`{"event_type":"order.created","payload":{}}`), nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestDispatcherRejectsCorrelationMismatch(t *testing.T) {
	d := dispatcher.New()
	resp := postToDispatcher(t, d, makeEnvelope(t, "order.created"),
		map[string]string{"X-Correlation-Id": "another-saga"})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestAuthenticateRequiresConfiguredMatchingToken(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })

	for _, tc := range []struct {
		name       string
		configured string
		provided   string
		want       int
	}{
		{name: "missing server configuration", want: http.StatusServiceUnavailable},
		{name: "missing caller token", configured: "secret", want: http.StatusUnauthorized},
		{name: "wrong caller token", configured: "secret", provided: "wrong", want: http.StatusUnauthorized},
		{name: "matching token", configured: "secret", provided: "secret", want: http.StatusNoContent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/dispatch", nil)
			req.Header.Set(dispatcher.DispatchTokenHeader, tc.provided)
			res := httptest.NewRecorder()
			dispatcher.Authenticate(tc.configured, next).ServeHTTP(res, req)
			assert.Equal(t, tc.want, res.Code)
		})
	}
}

// (g) event_type sem handler → 200 silencioso (ACK, sem chamar nada)
func TestDispatcherUnknownEventTypeReturns200Silent(t *testing.T) {
	d := dispatcher.New()
	// nenhum Handle registrado

	resp := postToDispatcher(t, d, makeEnvelope(t, "some.random.event"), nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode,
		"event_type sem handler → ACK; Gateway avança offset")
}

// (h) CorrelationIDFromContext(ctx) dentro do handler
func TestDispatcherCorrelationIDInjectedIntoContext(t *testing.T) {
	d := dispatcher.New()
	var captured string
	d.Handle("order.created", func(ctx context.Context, _ dispatcher.Event) error {
		captured = dispatcher.CorrelationIDFromContext(ctx)
		return nil
	})

	resp := postToDispatcher(t, d, makeEnvelope(t, "order.created"),
		map[string]string{"X-Correlation-Id": "saga-dispatch"})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "saga-dispatch", captured)
}

func TestDispatcherHandleReplacesPrevious(t *testing.T) {
	d := dispatcher.New()
	callsFirst := 0
	callsSecond := 0
	d.Handle("order.created", func(_ context.Context, _ dispatcher.Event) error {
		callsFirst++
		return nil
	})
	d.Handle("order.created", func(_ context.Context, _ dispatcher.Event) error {
		callsSecond++
		return nil
	})

	resp := postToDispatcher(t, d, makeEnvelope(t, "order.created"), nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 0, callsFirst, "primeiro handler substituído")
	assert.Equal(t, 1, callsSecond)
}

func TestCorrelationIDFromContextEmptyWhenNotInDispatcherFlow(t *testing.T) {
	assert.Empty(t, dispatcher.CorrelationIDFromContext(context.Background()),
		"fora do ServeHTTP: string vazia, sem panic")
}
