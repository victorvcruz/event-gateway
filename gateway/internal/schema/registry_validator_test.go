package schema

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validatorTestSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","required":["event_type"],"properties":{"event_type":{"const":"order.created"}},"additionalProperties":false}`

func latestResponse() string {
	return fmt.Sprintf(`{"subject":"tcc-poc.order.created-value","id":1,"version":1,"schemaType":"JSON","schema":%q}`, validatorTestSchema)
}

func TestRegistryValidatorCachesAndRefreshesOnlyOnceConcurrently(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		time.Sleep(20 * time.Millisecond)
		_, _ = w.Write([]byte(latestResponse()))
	}))
	defer server.Close()
	client, err := NewRegistryClient(server.URL, time.Second, nil)
	require.NoError(t, err)
	validator, err := NewRegistryValidator(client, RegistryValidatorConfig{
		GroupName: "tcc-poc", CacheEnabled: true, CacheTTL: time.Minute,
	})
	require.NoError(t, err)

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- validator.Validate(context.Background(), "order.created", []byte(`{"event_type":"order.created"}`))
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	assert.Equal(t, int32(1), requests.Load(), "concurrent cache misses must share one refresh")
}

func TestRegistryValidatorNeverUsesExpiredSchemaWhenRefreshFails(t *testing.T) {
	now := time.Now()
	available := atomic.Bool{}
	available.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !available.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(latestResponse()))
	}))
	defer server.Close()
	client, err := NewRegistryClient(server.URL, time.Second, nil)
	require.NoError(t, err)
	validator, err := NewRegistryValidator(client, RegistryValidatorConfig{
		GroupName: "tcc-poc", CacheEnabled: true, CacheTTL: time.Minute, Clock: func() time.Time { return now },
	})
	require.NoError(t, err)

	require.NoError(t, validator.Validate(context.Background(), "order.created", []byte(`{"event_type":"order.created"}`)))
	now = now.Add(2 * time.Minute)
	available.Store(false)

	err = validator.Validate(context.Background(), "order.created", []byte(`{"event_type":"order.created"}`))
	assert.ErrorIs(t, err, ErrRegistryUnavailable)
}

func TestRegistryValidatorReportsContractViolation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(latestResponse()))
	}))
	defer server.Close()
	client, err := NewRegistryClient(server.URL, time.Second, nil)
	require.NoError(t, err)
	validator, err := NewRegistryValidator(client, RegistryValidatorConfig{GroupName: "tcc-poc"})
	require.NoError(t, err)

	err = validator.Validate(context.Background(), "order.created", []byte(`{"event_type":"other"}`))
	assert.ErrorIs(t, err, ErrSchemaViolation)
}
