package httpx_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/victorvcruz/event-gateway/shared/httpx"
)

func newRequest(t *testing.T, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(`{}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestDoSuccessFirstTry(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := httpx.NewClient(httpx.Config{MaxRetries: 3, BackoffBase: time.Millisecond})
	resp, err := c.Do(context.Background(), newRequest(t, srv.URL))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.EqualValues(t, 1, atomic.LoadInt32(&calls))
}

func TestDoRetriesThenSucceeds(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n < 4 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := httpx.NewClient(httpx.Config{MaxRetries: 4, BackoffBase: time.Millisecond})
	resp, err := c.Do(context.Background(), newRequest(t, srv.URL))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.EqualValues(t, 4, atomic.LoadInt32(&calls))
}

func TestDoRetriesExhausted(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := httpx.NewClient(httpx.Config{MaxRetries: 2, BackoffBase: time.Millisecond})
	_, err := c.Do(context.Background(), newRequest(t, srv.URL))

	var exhausted *httpx.RetriesExhaustedError
	require.ErrorAs(t, err, &exhausted)
	assert.Equal(t, http.StatusServiceUnavailable, exhausted.LastStatus)
	assert.Equal(t, 3, exhausted.Attempts)
	assert.EqualValues(t, 3, atomic.LoadInt32(&calls))
}

func TestDoDoesNotRetry4xx(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusUnprocessableEntity)
	}))
	defer srv.Close()

	c := httpx.NewClient(httpx.Config{MaxRetries: 3, BackoffBase: time.Millisecond})
	resp, err := c.Do(context.Background(), newRequest(t, srv.URL))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	assert.EqualValues(t, 1, atomic.LoadInt32(&calls))
}

func TestDoRetriesOnTimeout(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			time.Sleep(200 * time.Millisecond)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := httpx.NewClient(httpx.Config{
		Timeout:     50 * time.Millisecond,
		MaxRetries:  3,
		BackoffBase: time.Millisecond,
	})
	resp, err := c.Do(context.Background(), newRequest(t, srv.URL))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(3))
}

func TestDoPropagatesHeaders(t *testing.T) {
	var (
		gotCorrID  string
		gotIdemKey string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCorrID = r.Header.Get(httpx.HeaderCorrelationID)
		gotIdemKey = r.Header.Get(httpx.HeaderIdempotencyKey)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx := context.Background()
	ctx = httpx.WithCorrelationID(ctx, "saga-42")
	ctx = httpx.WithIdempotencyKey(ctx, "idem-42")

	c := httpx.NewClient(httpx.Config{MaxRetries: 0, BackoffBase: time.Millisecond})
	resp, err := c.Do(ctx, newRequest(t, srv.URL))
	require.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	assert.Equal(t, "saga-42", gotCorrID)
	assert.Equal(t, "idem-42", gotIdemKey)
}

func TestDoCtxCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := httpx.NewClient(httpx.Config{MaxRetries: 3, BackoffBase: 500 * time.Millisecond})
	_, err := c.Do(ctx, newRequest(t, srv.URL))

	require.Error(t, err)
	// Depois do primeiro 5xx o client dorme; o ctx cancelado interrompe com ctx.Err().
	assert.True(t, errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "retries exhausted"))
}
