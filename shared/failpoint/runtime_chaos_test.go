//go:build chaos

package failpoint

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestChaosProbeKillsOnlyAfterControllerDecision(t *testing.T) {
	var got hitRequest
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		require.NoError(t, json.NewDecoder(request.Body).Decode(&got))
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"action":"kill"}`)), Header: make(http.Header)}, nil
	})}
	killed := false
	probe := &httpProbe{processID: "worker", endpoint: "http://controller", client: client, log: zap.NewNop(), kill: func() error { killed = true; return nil }}
	probe.Hit(context.Background(), Checkpoint{Point: OutboxAfterSendACK, CorrelationID: "order-1"})
	require.True(t, killed)
	require.Equal(t, "worker", got.Process)
	require.Equal(t, "order-1", got.CorrelationID)
}

func TestChaosProbeFailsOpen(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("controller unavailable") })}
	probe := &httpProbe{processID: "worker", endpoint: "http://controller", client: client, log: zap.NewNop(), kill: func() error { t.Fatal("must not kill"); return nil }}
	require.NotPanics(t, func() { probe.Hit(context.Background(), Checkpoint{Point: OutboxAfterSendACK}) })
}
