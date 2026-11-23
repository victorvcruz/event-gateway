package delivery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/shared/outbox"
)

//go:generate mockgen -source=$GOFILE -destination=./mocks/mock_httpdoer.go -package=mocks

// HTTPDoer é o mínimo que o HTTPSender precisa de um cliente HTTP.
// shared/httpx.Client atende.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// HTTPSender implementa outbox.Sender fazendo POST no callback_url do wrapper
// DispatchEnvelope. Faz uma tentativa só; o retry fica com o Worker, via
// retry_count e BackoffSchedule.
type HTTPSender struct {
	doer          HTTPDoer
	log           *zap.Logger
	dispatchToken string
}

func (s *HTTPSender) WithDispatchToken(token string) *HTTPSender {
	s.dispatchToken = token
	return s
}

func NewHTTPSender(doer HTTPDoer) *HTTPSender {
	return &HTTPSender{doer: doer, log: zap.NewNop()}
}

// WithLogger injeta um logger opcional. Sem chamada o Sender fica silencioso.
func (s *HTTPSender) WithLogger(l *zap.Logger) *HTTPSender {
	if l != nil {
		s.log = l
	}
	return s
}

// Send decodifica o wrapper de row.Payload e faz POST no callback_url com o
// envelope cru como body (o wrapper não vai para o subscriber). A resposta é
// classificada assim:
//   - 2xx e 409             → nil (ACK, o Worker marca SENT)
//   - 400/404/410/422       → ErrNonRetryable (vai direto para DEAD)
//   - 5xx/408/425/429/erro  → erro comum (o Worker tenta de novo com backoff)
//   - wrapper corrompido    → ErrNonRetryable (retry não conserta)
func (s *HTTPSender) Send(ctx context.Context, row outbox.Row) error {
	env, err := DecodeDispatchEnvelope(row.Payload)
	if err != nil {
		return fmt.Errorf("%w: decode wrapper: %v", outbox.ErrNonRetryable, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		env.Target.CallbackURL, bytes.NewReader(env.Envelope))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	deliveryID := uuid.NewString()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Correlation-Id", row.CorrelationID)
	req.Header.Set("X-Gateway-Delivery-Id", deliveryID)
	if s.dispatchToken != "" {
		req.Header.Set("X-Gateway-Token", s.dispatchToken)
	}

	baseFields := []zap.Field{
		zap.String("event_type", row.EventType),
		zap.String("event_id", row.EventID.String()),
		zap.String("correlation_id", row.CorrelationID),
		zap.String("service_id", env.Target.ServiceID),
		zap.String("callback_url", env.Target.CallbackURL),
		zap.String("delivery_id", deliveryID),
		zap.Int("retry_count", row.RetryCount),
	}

	start := time.Now()
	resp, err := s.doer.Do(req)
	if err != nil {
		s.log.Warn("dispatch network error",
			append(baseFields, zap.Duration("latency", time.Since(start)), zap.Error(err))...)
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	classifyErr := classifyResponse(resp.StatusCode, body, env.Target.CallbackURL)
	logFields := append(baseFields,
		zap.Int("status", resp.StatusCode),
		zap.Duration("latency", time.Since(start)))
	switch {
	case classifyErr == nil:
		s.log.Info("dispatch ack", logFields...)
	case errors.Is(classifyErr, outbox.ErrNonRetryable):
		s.log.Warn("dispatch non-retryable", append(logFields, zap.String("body", snippet(body)))...)
	default:
		s.log.Warn("dispatch retryable failure", append(logFields, zap.String("body", snippet(body)))...)
	}
	return classifyErr
}

func classifyResponse(status int, body []byte, url string) error {
	switch {
	case status >= 200 && status < 300:
		return nil
	case status == http.StatusConflict:
		return nil
	case isTerminal(status):
		return fmt.Errorf("%w: %d from %s: %s",
			outbox.ErrNonRetryable, status, url, snippet(body))
	default:
		return fmt.Errorf("http %d from %s: %s", status, url, snippet(body))
	}
}

// Quando isTerminal é true, o subscriber declarou falha permanente e tentar de
// novo não muda o resultado. Os outros 4xx caem no retry comum de
// classifyResponse.
func isTerminal(status int) bool {
	return status == http.StatusBadRequest ||
		status == http.StatusNotFound ||
		status == http.StatusGone ||
		status == http.StatusUnprocessableEntity
}

// snippet corta o body em 200 caracteres para não encher o log.
func snippet(body []byte) string {
	if len(body) > 200 {
		return string(body[:200]) + "..."
	}
	return string(body)
}

// Erros sentinela para quem precisa distinguir as classes de falha (por
// exemplo, outcome=retry vs dead na métrica). errors.Is funciona com wrap.
var (
	ErrRetryable = errors.New("dispatch: retryable failure")
)
