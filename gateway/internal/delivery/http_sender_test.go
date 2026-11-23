package delivery_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/victorvcruz/event-gateway/gateway/internal/delivery"
	"github.com/victorvcruz/event-gateway/shared/outbox"
)

type stubDoer struct {
	seenReqs []*http.Request
	respond  func(*http.Request) (*http.Response, error)
}

func (s *stubDoer) Do(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		// Ler e recolocar o body para permitir asserts posteriores
		buf, _ := io.ReadAll(req.Body)
		_ = req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(buf))
	}
	s.seenReqs = append(s.seenReqs, req)
	return s.respond(req)
}

func newRow(t *testing.T, target delivery.TargetMeta, envelope json.RawMessage, corr string) outbox.Row {
	t.Helper()
	wrapped, err := delivery.EncodeDispatchEnvelope(target, envelope)
	require.NoError(t, err)
	return outbox.Row{
		EventID:       uuid.New(),
		EventType:     "order.created",
		CorrelationID: corr,
		Payload:       wrapped,
	}
}

func makeResp(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Header:     make(http.Header),
	}
}

func TestSendStatusMatrix(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		nonRetry bool
		ack      bool
	}{
		{"200 OK ACK", 200, false, true},
		{"201 Created ACK", 201, false, true},
		{"202 Accepted ACK", 202, false, true},
		{"204 No Content ACK", 204, false, true},
		{"409 Conflict ACK", 409, false, true},
		{"400 Bad Request DEAD", 400, true, false},
		{"404 Not Found DEAD", 404, true, false},
		{"410 Gone DEAD", 410, true, false},
		{"422 Unprocessable DEAD", 422, true, false},
		{"408 Request Timeout RETRY", 408, false, false},
		{"425 Too Early RETRY", 425, false, false},
		{"429 Too Many Requests RETRY", 429, false, false},
		{"500 Server Error RETRY", 500, false, false},
		{"502 Bad Gateway RETRY", 502, false, false},
		{"503 Unavailable RETRY", 503, false, false},
		{"504 Gateway Timeout RETRY", 504, false, false},
	}
	target := delivery.TargetMeta{ServiceID: "svc", CallbackURL: "http://svc/dispatch"}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			doer := &stubDoer{
				respond: func(*http.Request) (*http.Response, error) {
					return makeResp(c.status, "body"), nil
				},
			}
			s := delivery.NewHTTPSender(doer)
			row := newRow(t, target, json.RawMessage(`{"x":1}`), "corr")

			err := s.Send(context.Background(), row)
			switch {
			case c.ack:
				assert.NoError(t, err, "status %d should ACK", c.status)
			case c.nonRetry:
				require.Error(t, err)
				assert.True(t, errors.Is(err, outbox.ErrNonRetryable),
					"status %d should wrap ErrNonRetryable", c.status)
			default:
				require.Error(t, err)
				assert.False(t, errors.Is(err, outbox.ErrNonRetryable),
					"status %d should NOT wrap ErrNonRetryable (retry-worthy)", c.status)
			}
		})
	}
}

func TestSendNetworkErrorIsRetryable(t *testing.T) {
	doer := &stubDoer{
		respond: func(*http.Request) (*http.Response, error) {
			return nil, errors.New("connection refused")
		},
	}
	s := delivery.NewHTTPSender(doer)
	row := newRow(t,
		delivery.TargetMeta{ServiceID: "x", CallbackURL: "http://x/dispatch"},
		json.RawMessage(`{}`), "c")

	err := s.Send(context.Background(), row)
	require.Error(t, err)
	assert.False(t, errors.Is(err, outbox.ErrNonRetryable),
		"erro de rede é retentável (não é falha permanente do subscriber)")
}

func TestSendMalformedWrapperIsNonRetryable(t *testing.T) {
	doer := &stubDoer{}
	s := delivery.NewHTTPSender(doer)
	row := outbox.Row{
		EventID:       uuid.New(),
		EventType:     "x",
		CorrelationID: "c",
		Payload:       json.RawMessage("not a wrapper"),
	}

	err := s.Send(context.Background(), row)
	require.Error(t, err)
	assert.True(t, errors.Is(err, outbox.ErrNonRetryable),
		"payload não-wrapper não recupera com retry")
	assert.Empty(t, doer.seenReqs, "não deve ter tentado HTTP")
}

func TestSendInjectsHeaders(t *testing.T) {
	var captured *http.Request
	doer := &stubDoer{
		respond: func(r *http.Request) (*http.Response, error) {
			captured = r
			return makeResp(200, ""), nil
		},
	}
	s := delivery.NewHTTPSender(doer).WithDispatchToken("dispatch-secret")
	env := json.RawMessage(`{"raw":"envelope"}`)
	row := newRow(t,
		delivery.TargetMeta{ServiceID: "payment", CallbackURL: "http://payment/dispatch"},
		env, "saga-xyz")

	require.NoError(t, s.Send(context.Background(), row))
	require.NotNil(t, captured)

	assert.Equal(t, "saga-xyz", captured.Header.Get("X-Correlation-Id"))
	assert.Equal(t, "dispatch-secret", captured.Header.Get("X-Gateway-Token"))
	deliveryID := captured.Header.Get("X-Gateway-Delivery-Id")
	assert.NotEmpty(t, deliveryID)
	_, err := uuid.Parse(deliveryID)
	assert.NoError(t, err, "delivery-id deve ser UUID válido")

	assert.Equal(t, "application/json", captured.Header.Get("Content-Type"))
	assert.Equal(t, "http://payment/dispatch", captured.URL.String())

	body, _ := io.ReadAll(captured.Body)
	assert.JSONEq(t, string(env), string(body),
		"body do POST é env.Envelope (raw); wrapper NUNCA vaza")
}

func TestSendContextTimeoutIsRetryableNotTerminal(t *testing.T) {
	doer := &stubDoer{
		respond: func(r *http.Request) (*http.Response, error) {
			// Simula subscriber lento: dorme mais que o timeout do context
			// e devolve o erro correto que o net/http.Client produziria.
			<-r.Context().Done()
			return nil, r.Context().Err()
		},
	}
	s := delivery.NewHTTPSender(doer)
	row := newRow(t,
		delivery.TargetMeta{ServiceID: "slow", CallbackURL: "http://slow/dispatch"},
		json.RawMessage(`{}`), "corr")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := s.Send(ctx, row)
	require.Error(t, err)
	assert.False(t, errors.Is(err, outbox.ErrNonRetryable),
		"context deadline exceeded é falha transitória; Worker deve retentar")
}

func TestSendDeliveryIDIsUUIDv4(t *testing.T) {
	var captured string
	doer := &stubDoer{
		respond: func(r *http.Request) (*http.Response, error) {
			captured = r.Header.Get("X-Gateway-Delivery-Id")
			return makeResp(200, ""), nil
		},
	}
	s := delivery.NewHTTPSender(doer)
	row := newRow(t,
		delivery.TargetMeta{ServiceID: "x", CallbackURL: "http://x/dispatch"},
		json.RawMessage(`{}`), "corr")
	require.NoError(t, s.Send(context.Background(), row))

	parsed, err := uuid.Parse(captured)
	require.NoError(t, err)
	assert.Equal(t, uuid.Version(4), parsed.Version(),
		"delivery-id deve ser UUIDv4 (contrato ia/middleware-contracts.md)")
}

func TestSendGeneratesDistinctDeliveryIDsPerAttempt(t *testing.T) {
	var ids []string
	doer := &stubDoer{
		respond: func(r *http.Request) (*http.Response, error) {
			ids = append(ids, r.Header.Get("X-Gateway-Delivery-Id"))
			return makeResp(500, "boom"), nil
		},
	}
	s := delivery.NewHTTPSender(doer)
	row := newRow(t,
		delivery.TargetMeta{ServiceID: "x", CallbackURL: "http://x/dispatch"},
		json.RawMessage(`{}`), "c")

	for range 3 {
		_ = s.Send(context.Background(), row)
	}
	require.Len(t, ids, 3)
	assert.NotEqual(t, ids[0], ids[1])
	assert.NotEqual(t, ids[1], ids[2])
	assert.NotEqual(t, ids[0], ids[2])
}
