package schema

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type requestObservation struct {
	operation string
	outcome   string
}

type recordingRegistryObserver struct {
	mu   sync.Mutex
	seen []requestObservation
}

func (o *recordingRegistryObserver) ObserveRegistryRequest(operation, outcome string, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.seen = append(o.seen, requestObservation{operation: operation, outcome: outcome})
}

func TestRegistryClientLatestEscapesSubjectAndDecodesJSONSchema(t *testing.T) {
	var escapedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		escapedPath = r.URL.EscapedPath()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"subject":"group.event/value","id":7,"version":2,"schemaType":"JSON","schema":"{\"type\":\"object\"}"}`))
	}))
	defer server.Close()

	client, err := NewRegistryClient(server.URL, time.Second, nil)
	require.NoError(t, err)
	latest, err := client.Latest(context.Background(), "group.event/value")
	require.NoError(t, err)

	assert.Equal(t, "/subjects/group.event%2Fvalue/versions/latest", escapedPath)
	assert.Equal(t, 7, latest.ID)
	assert.Equal(t, 2, latest.Version)
	assert.JSONEq(t, `{"type":"object"}`, string(latest.Schema))
}

func TestRegistryClientClassifiesNotFoundAndUnavailable(t *testing.T) {
	tests := []struct {
		name   string
		status int
		want   error
		result string
	}{
		{name: "missing subject", status: http.StatusNotFound, want: ErrSchemaNotFound, result: "not_found"},
		{name: "registry failure", status: http.StatusServiceUnavailable, want: ErrRegistryUnavailable, result: "unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observer := &recordingRegistryObserver{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(`{"error_code":40401,"message":"failure"}`))
			}))
			defer server.Close()
			client, err := NewRegistryClient(server.URL, time.Second, observer)
			require.NoError(t, err)
			_, err = client.Latest(context.Background(), "subject")
			assert.ErrorIs(t, err, tt.want)
			require.Len(t, observer.seen, 1)
			assert.Equal(t, tt.result, observer.seen[0].outcome)
		})
	}
}

func TestRegistryClientRequestTimeoutIsUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		time.Sleep(100 * time.Millisecond)
	}))
	defer server.Close()
	client, err := NewRegistryClient(server.URL, 10*time.Millisecond, nil)
	require.NoError(t, err)

	err = client.Ping(context.Background())
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrRegistryUnavailable))
}
