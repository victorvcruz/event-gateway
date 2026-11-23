package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/victorvcruz/event-gateway/gateway/pkg/client"
)

type stubDoer struct {
	req     *http.Request
	reqBody []byte
	respond func(*http.Request) (*http.Response, error)
}

func (s *stubDoer) Do(r *http.Request) (*http.Response, error) {
	if r.Body != nil {
		b, _ := io.ReadAll(r.Body)
		s.reqBody = b
		r.Body = io.NopCloser(bytes.NewReader(b))
	}
	s.req = r
	return s.respond(r)
}

func makeResp(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Header:     make(http.Header),
	}
}

func newEvent(t *testing.T) client.Event {
	t.Helper()
	return client.Event{
		EventID:       uuid.New(),
		EventType:     "order.created",
		CorrelationID: "saga-1",
		Payload:       json.RawMessage(`{"order_id":"x","amount":9900}`),
		OccurredAt:    time.Now().UTC().Truncate(time.Second),
	}
}

func TestNewValidatesURL(t *testing.T) {
	cases := []struct {
		name string
		url  string
	}{
		{"empty", ""},
		{"no scheme", "gateway:8080"},
		{"no host", "http://"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := client.New(c.url)
			assert.Error(t, err)
		})
	}
}

func TestPublishEventStatusMatrix(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		body     string
		outcome  client.Outcome
		errCheck func(err error) bool
	}{
		{"202 Accepted", 202, "", client.OutcomeAccepted, func(err error) bool { return err == nil }},
		{"409 Duplicate", 409, "", client.OutcomeDuplicate, func(err error) bool { return err == nil }},
		{"422 Validation", 422, "field 'amount' missing", 0, func(err error) bool {
			var vErr *client.ValidationError
			return errors.As(err, &vErr) && vErr.Body == "field 'amount' missing"
		}},
		{"400 BadRequest", 400, "malformed", 0, func(err error) bool {
			return errors.Is(err, client.ErrBadRequest)
		}},
		{"500 Server", 500, "", 0, func(err error) bool {
			return errors.Is(err, client.ErrGatewayUnavailable)
		}},
		{"503 Unavailable", 503, "", 0, func(err error) bool {
			return errors.Is(err, client.ErrGatewayUnavailable)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			doer := &stubDoer{respond: func(*http.Request) (*http.Response, error) {
				return makeResp(c.status, c.body), nil
			}}
			cli, err := client.New("http://gateway:8080", client.WithHTTPClient(doer))
			require.NoError(t, err)

			res, err := cli.PublishEvent(context.Background(), newEvent(t))
			if c.outcome != 0 {
				require.NoError(t, err)
				assert.Equal(t, c.outcome, res.Outcome)
			}
			assert.True(t, c.errCheck(err), "err classification failed for %d: %v", c.status, err)
		})
	}
}

func TestValidationErrorErrorsIsAndAs(t *testing.T) {
	doer := &stubDoer{respond: func(*http.Request) (*http.Response, error) {
		return makeResp(422, "schema violation on payload.amount"), nil
	}}
	cli, err := client.New("http://gateway", client.WithHTTPClient(doer))
	require.NoError(t, err)

	_, err = cli.PublishEvent(context.Background(), newEvent(t))
	require.Error(t, err)
	assert.True(t, errors.Is(err, client.ErrValidationFailed),
		"errors.Is deve reconhecer ErrValidationFailed via ValidationError.Is")

	var vErr *client.ValidationError
	require.True(t, errors.As(err, &vErr))
	assert.Equal(t, "schema violation on payload.amount", vErr.Body)
}

func TestPublishEventInjectsCorrelationHeader(t *testing.T) {
	doer := &stubDoer{respond: func(*http.Request) (*http.Response, error) {
		return makeResp(202, ""), nil
	}}
	cli, err := client.New("http://gateway", client.WithHTTPClient(doer))
	require.NoError(t, err)

	evt := newEvent(t)
	evt.CorrelationID = "saga-xyz"
	_, err = cli.PublishEvent(context.Background(), evt)
	require.NoError(t, err)

	assert.Equal(t, "saga-xyz", doer.req.Header.Get("X-Correlation-Id"))
	assert.Equal(t, "application/json", doer.req.Header.Get("Content-Type"))
	assert.Equal(t, "http://gateway/events", doer.req.URL.String())
}

func TestPublishEventEmptyCorrelationIDIsBadRequestWithoutNetworkCall(t *testing.T) {
	doer := &stubDoer{respond: func(*http.Request) (*http.Response, error) {
		t.Fatal("should not reach the network")
		return nil, nil
	}}
	cli, err := client.New("http://gateway", client.WithHTTPClient(doer))
	require.NoError(t, err)

	evt := newEvent(t)
	evt.CorrelationID = ""
	_, err = cli.PublishEvent(context.Background(), evt)
	require.Error(t, err)
	assert.True(t, errors.Is(err, client.ErrBadRequest))
	assert.Nil(t, doer.req, "nenhum request enviado à rede")
}

func TestPublishEventEmptyEventTypeIsBadRequest(t *testing.T) {
	doer := &stubDoer{respond: func(*http.Request) (*http.Response, error) {
		t.Fatal("should not reach the network")
		return nil, nil
	}}
	cli, err := client.New("http://gateway", client.WithHTTPClient(doer))
	require.NoError(t, err)

	evt := newEvent(t)
	evt.EventType = ""
	_, err = cli.PublishEvent(context.Background(), evt)
	require.Error(t, err)
	assert.True(t, errors.Is(err, client.ErrBadRequest))
}

func TestPublishEventNetworkErrorIsGatewayUnavailable(t *testing.T) {
	doer := &stubDoer{respond: func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("connection refused")
	}}
	cli, err := client.New("http://gateway", client.WithHTTPClient(doer))
	require.NoError(t, err)

	_, err = cli.PublishEvent(context.Background(), newEvent(t))
	require.Error(t, err)
	assert.True(t, errors.Is(err, client.ErrGatewayUnavailable))
}

// Roundtrip: detecta divergência entre as struct tags e
// wire format esperado pelo Gateway.
func TestEnvelopeRoundtrip(t *testing.T) {
	orig := client.Event{
		EventID:       uuid.New(),
		EventType:     "payment.processed",
		CorrelationID: "saga-round",
		Payload:       json.RawMessage(`{"amount":10000,"currency":"BRL"}`),
		OccurredAt:    time.Date(2026, 8, 11, 14, 0, 0, 0, time.UTC),
	}
	encoded, err := json.Marshal(orig)
	require.NoError(t, err)

	// verificar campos esperados no JSON (wire format real conforme schemas)
	var m map[string]any
	require.NoError(t, json.Unmarshal(encoded, &m))
	assert.Contains(t, m, "event_id")
	assert.Contains(t, m, "event_type")
	assert.Contains(t, m, "correlation_id")
	assert.Contains(t, m, "payload")
	assert.Contains(t, m, "timestamp")

	var decoded client.Event
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, orig.EventID, decoded.EventID)
	assert.Equal(t, orig.EventType, decoded.EventType)
	assert.Equal(t, orig.CorrelationID, decoded.CorrelationID)
	assert.JSONEq(t, string(orig.Payload), string(decoded.Payload))
	assert.True(t, orig.OccurredAt.Equal(decoded.OccurredAt))
}
