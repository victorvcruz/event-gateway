package schema

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedCatalogContainsAllNineConfiguredContracts(t *testing.T) {
	catalog, err := LoadEmbeddedCatalog(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{
		"order.cancelled", "order.created", "order.fulfilled",
		"payment.failed", "payment.processed", "payment.refunded",
		"stock.failed", "stock.released", "stock.reserved",
	}, catalog.EventTypes())
}

func TestProvisionerIsIdempotentWhenLatestSchemaMatches(t *testing.T) {
	raw := []byte(`{"$schema":"http://json-schema.org/draft-07/schema#","$id":"order.created","type":"object"}`)
	var registrations atomic.Int32
	var compatibilityUpdates atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_, _ = fmt.Fprintf(w, `{"subject":"tcc-poc.order.created-value","id":10,"version":1,"schemaType":"JSON","schema":%q}`, raw)
		case http.MethodPut:
			compatibilityUpdates.Add(1)
			_, _ = w.Write([]byte(`{"compatibility":"FULL_TRANSITIVE"}`))
		case http.MethodPost:
			registrations.Add(1)
			_, _ = w.Write([]byte(`{"id":11}`))
		}
	}))
	defer server.Close()
	client, err := NewRegistryClient(server.URL, time.Second, nil)
	require.NoError(t, err)
	provisioner, err := NewProvisioner(client, "tcc-poc", "FULL_TRANSITIVE")
	require.NoError(t, err)

	results, err := provisioner.Provision(context.Background(), Catalog{"order.created": raw}, []string{"order.created"})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.False(t, results[0].Created)
	assert.Equal(t, int32(0), registrations.Load())
	assert.Equal(t, int32(1), compatibilityUpdates.Load())
}

func TestProvisionerRejectsIncompatibleEvolutionBeforeRegistration(t *testing.T) {
	oldSchema := []byte(`{"$schema":"http://json-schema.org/draft-07/schema#","$id":"order.created","type":"object"}`)
	newSchema := []byte(`{"$schema":"http://json-schema.org/draft-07/schema#","$id":"order.created","type":"string"}`)
	var registrations atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_, _ = fmt.Fprintf(w, `{"subject":"tcc-poc.order.created-value","id":10,"version":1,"schemaType":"JSON","schema":%q}`, oldSchema)
		case http.MethodPut:
			_, _ = w.Write([]byte(`{"compatibility":"FULL_TRANSITIVE"}`))
		case http.MethodPost:
			if strings.HasPrefix(r.URL.Path, "/compatibility/") {
				_, _ = w.Write([]byte(`{"is_compatible":false,"messages":["type changed"]}`))
			} else {
				registrations.Add(1)
				_, _ = w.Write([]byte(`{"id":11}`))
			}
		}
	}))
	defer server.Close()
	client, err := NewRegistryClient(server.URL, time.Second, nil)
	require.NoError(t, err)
	provisioner, err := NewProvisioner(client, "tcc-poc", "FULL_TRANSITIVE")
	require.NoError(t, err)

	_, err = provisioner.Provision(context.Background(), Catalog{"order.created": newSchema}, []string{"order.created"})
	assert.True(t, errors.Is(err, ErrIncompatibleSchema))
	assert.Equal(t, int32(0), registrations.Load())
}
