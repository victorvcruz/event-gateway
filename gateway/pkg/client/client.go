// Package client é o SDK Go para chamar POST /events no gateway. Os serviços
// de negócio o usam sem depender de nenhum SDK do Kafka; só importa a
// biblioteca padrão e google/uuid.
//
// Uso típico num dispatcher em background:
//
//	gw, _ := client.New("http://gateway:8080")
//	res, err := gw.PublishEvent(ctx, client.Event{...})
//	switch {
//	case err == nil:
//	    repo.MarkOutboxSent(...)          // 202 ou 409
//	case errors.Is(err, client.ErrValidationFailed):
//	    repo.MarkOutboxInvalid(...)       // não retentar
//	default:
//	    return err                         // manter PENDING, próximo tick retenta
//	}
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Event é o envelope esperado pelo gateway em POST /events. As tags seguem o
// formato validado pelos JSON Schemas, em que o campo é "timestamp"; no Go o
// nome é OccurredAt, que lê melhor.
type Event struct {
	EventID       uuid.UUID       `json:"event_id"`
	EventType     string          `json:"event_type"`
	CorrelationID string          `json:"correlation_id"`
	Payload       json.RawMessage `json:"payload"`
	OccurredAt    time.Time       `json:"timestamp"`
}

// Outcome diz o que aconteceu num PublishEvent sem erro.
type Outcome int

const (
	// OutcomeAccepted → Gateway respondeu 202 (linha nova em outbox).
	OutcomeAccepted Outcome = iota + 1
	// OutcomeDuplicate → Gateway respondeu 409 (idempotência; sucesso).
	OutcomeDuplicate
)

type PublishResult struct {
	Outcome Outcome
}

var (
	// ErrValidationFailed → Gateway respondeu 422 (schema não bate).
	// Callers usam errors.As com *ValidationError para ler o Problem Details.
	ErrValidationFailed = errors.New("gateway: event failed schema validation")
	// ErrBadRequest → Gateway respondeu 400 (envelope malformado).
	ErrBadRequest = errors.New("gateway: malformed envelope")
	// ErrGatewayUnavailable → 5xx, timeout, erro de rede, DNS, TLS.
	// Callers tipicamente mantêm status=PENDING para o próximo tick.
	ErrGatewayUnavailable = errors.New("gateway: unavailable — retry later")
)

// ValidationError embrulha ErrValidationFailed e preserva o body
// application/problem+json devolvido pelo Gateway.
type ValidationError struct {
	Body string
}

func (e *ValidationError) Error() string { return "gateway: " + e.Body }
func (e *ValidationError) Is(target error) bool {
	return target == ErrValidationFailed
}

// Client faz POST /events. É seguro para uso concorrente; reaproveite a instância.
type Client struct {
	base *url.URL
	doer HTTPDoer
}

// New cria um Client para o gatewayURL (ex.: "http://gateway:8080").
func New(gatewayURL string, opts ...Option) (*Client, error) {
	if gatewayURL == "" {
		return nil, errors.New("client: gatewayURL is required")
	}
	base, err := url.Parse(gatewayURL)
	if err != nil {
		return nil, fmt.Errorf("client: parse gatewayURL: %w", err)
	}
	if base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("client: gatewayURL must include scheme and host: %q", gatewayURL)
	}

	o := options{timeout: 5 * time.Second}
	for _, opt := range opts {
		opt(&o)
	}
	if o.doer == nil {
		o.doer = &http.Client{Timeout: o.timeout}
	}
	return &Client{base: base, doer: o.doer}, nil
}

// PublishEvent faz POST /events com o envelope e envia evt.CorrelationID no
// header X-Correlation-Id. O retorno depende da resposta:
//
//	202 → PublishResult{OutcomeAccepted}, nil
//	409 → PublishResult{OutcomeDuplicate}, nil
//	422 → nil, *ValidationError (errors.Is → ErrValidationFailed)
//	400 → nil, ErrBadRequest
//	5xx / timeout / erro de rede → nil, ErrGatewayUnavailable
func (c *Client) PublishEvent(ctx context.Context, evt Event) (PublishResult, error) {
	if evt.CorrelationID == "" {
		return PublishResult{}, fmt.Errorf("%w: correlation_id is required", ErrBadRequest)
	}
	if evt.EventType == "" {
		return PublishResult{}, fmt.Errorf("%w: event_type is required", ErrBadRequest)
	}

	body, err := json.Marshal(evt)
	if err != nil {
		return PublishResult{}, fmt.Errorf("marshal event: %w", err)
	}

	endpoint := c.base.ResolveReference(&url.URL{Path: "/events"})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return PublishResult{}, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Correlation-Id", evt.CorrelationID)

	resp, err := c.doer.Do(req)
	if err != nil {
		return PublishResult{}, fmt.Errorf("%w: %v", ErrGatewayUnavailable, err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	return classifyResponse(resp.StatusCode, respBody)
}

func classifyResponse(status int, body []byte) (PublishResult, error) {
	switch {
	case status == http.StatusAccepted:
		return PublishResult{Outcome: OutcomeAccepted}, nil
	case status == http.StatusConflict:
		return PublishResult{Outcome: OutcomeDuplicate}, nil
	case status == http.StatusUnprocessableEntity:
		return PublishResult{}, &ValidationError{Body: strings.TrimSpace(string(body))}
	case status == http.StatusBadRequest:
		return PublishResult{}, fmt.Errorf("%w: %s", ErrBadRequest, strings.TrimSpace(string(body)))
	case status >= 500:
		return PublishResult{}, fmt.Errorf("%w: HTTP %d", ErrGatewayUnavailable, status)
	default:
		return PublishResult{}, fmt.Errorf("gateway: unexpected status %d: %s",
			status, strings.TrimSpace(string(body)))
	}
}
