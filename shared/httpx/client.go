// Package httpx é um cliente HTTP com backoff exponencial que propaga
// X-Correlation-Id e Idempotency-Key. Os dispatchers dos serviços o usam.
package httpx

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"time"
)

const (
	HeaderCorrelationID  = "X-Correlation-Id"
	HeaderIdempotencyKey = "Idempotency-Key"
)

type Config struct {
	Timeout     time.Duration
	MaxRetries  int
	BackoffBase time.Duration
	Jitter      bool
}

func (c Config) withDefaults() Config {
	if c.Timeout == 0 {
		c.Timeout = 5 * time.Second
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 3
	}
	if c.BackoffBase == 0 {
		c.BackoffBase = 100 * time.Millisecond
	}
	return c
}

type Client struct {
	inner *http.Client
	cfg   Config
}

func NewClient(cfg Config) *Client {
	cfg = cfg.withDefaults()
	return &Client{
		inner: &http.Client{Timeout: cfg.Timeout},
		cfg:   cfg,
	}
}

type RetriesExhaustedError struct {
	LastStatus int
	LastErr    error
	Attempts   int
}

func (e *RetriesExhaustedError) Error() string {
	if e.LastErr != nil {
		return fmt.Sprintf("httpx: retries exhausted after %d attempts: %v", e.Attempts, e.LastErr)
	}
	return fmt.Sprintf("httpx: retries exhausted after %d attempts (last status %d)", e.Attempts, e.LastStatus)
}

func (e *RetriesExhaustedError) Unwrap() error { return e.LastErr }

func (c *Client) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	if id, ok := CorrelationID(ctx); ok {
		req.Header.Set(HeaderCorrelationID, id)
	}
	if key, ok := IdempotencyKey(ctx); ok {
		req.Header.Set(HeaderIdempotencyKey, key)
	}

	var (
		lastResp   *http.Response
		lastErr    error
		lastStatus int
	)

	attempts := c.cfg.MaxRetries + 1
	for attempt := range attempts {
		if attempt > 0 {
			if err := sleep(ctx, c.backoff(attempt-1)); err != nil {
				return nil, err
			}
		}

		attemptReq := req.Clone(ctx)
		if req.GetBody != nil {
			body, gbErr := req.GetBody()
			if gbErr != nil {
				return nil, fmt.Errorf("httpx: reset body for retry: %w", gbErr)
			}
			attemptReq.Body = body
		}
		resp, err := c.inner.Do(attemptReq)
		if err != nil {
			if !isRetryable(err) {
				return nil, err
			}
			lastErr = err
			lastStatus = 0
			continue
		}

		if resp.StatusCode < 500 {
			return resp, nil
		}

		lastResp = resp
		lastStatus = resp.StatusCode
		lastErr = nil
		_ = resp.Body.Close()
	}

	if lastResp != nil {
		return nil, &RetriesExhaustedError{LastStatus: lastStatus, Attempts: attempts}
	}
	return nil, &RetriesExhaustedError{LastErr: lastErr, Attempts: attempts}
}

func (c *Client) backoff(attempt int) time.Duration {
	d := c.cfg.BackoffBase * (1 << attempt)
	if c.cfg.Jitter {
		j := time.Duration(rand.Int64N(int64(c.cfg.BackoffBase)))
		d += j
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return true
}
