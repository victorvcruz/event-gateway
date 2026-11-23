package experimentcontrol

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	controlmigrations "github.com/victorvcruz/event-gateway/gateway/internal/experimentcontrol/migrations"
	"github.com/victorvcruz/event-gateway/shared/contracts"
	"github.com/victorvcruz/event-gateway/shared/testpostgres"
)

func TestPostgresControlStoreHasNoOutboxAndIsIdempotent(t *testing.T) {
	harness := testpostgres.Start(t)
	require.NoError(t, controlmigrations.Apply(context.Background(), harness.Pool))
	var outboxTable *string
	require.NoError(t, harness.Pool.QueryRow(context.Background(),
		"SELECT to_regclass('public.outbox')::text").Scan(&outboxTable))
	require.Nil(t, outboxTable, "no-Outbox baseline must not create an outbox table")

	store := NewPostgresStore(harness.Pool)
	order := Order{
		OrderID: uuid.New(), EventID: uuid.New(), IdempotencyKey: uuid.NewString(),
		RunID: "integration", CustomerID: "customer", AmountCents: 1000,
		Items: []contracts.Item{{SKU: "SKU-001", Quantity: 1}}, EventTimestamp: time.Now().UTC(),
	}
	first, created, err := store.Create(context.Background(), order)
	require.NoError(t, err)
	require.True(t, created)
	second, created, err := store.Create(context.Background(), Order{
		OrderID: uuid.New(), EventID: uuid.New(), IdempotencyKey: order.IdempotencyKey,
		RunID: "changed", CustomerID: "changed", AmountCents: 2000,
		Items: []contracts.Item{{SKU: "OTHER", Quantity: 1}}, EventTimestamp: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, first.OrderID, second.OrderID)
	require.Equal(t, first.EventID, second.EventID)
	var count int
	require.NoError(t, harness.Pool.QueryRow(context.Background(),
		"SELECT count(*) FROM control_orders WHERE idempotency_key=$1", order.IdempotencyKey).Scan(&count))
	require.Equal(t, 1, count)

	_, _, err = store.Create(context.Background(), Order{
		OrderID: uuid.New(), EventID: uuid.New(), IdempotencyKey: uuid.NewString(),
		RunID: "integration", CustomerID: "invalid", AmountCents: -1,
		Items: []contracts.Item{{SKU: "SKU-001", Quantity: 1}}, EventTimestamp: time.Now().UTC(),
	})
	require.Error(t, err)
	require.NoError(t, harness.Pool.QueryRow(context.Background(),
		"SELECT count(*) FROM control_orders WHERE customer_id='invalid'").Scan(&count))
	require.Zero(t, count, "failed transaction must not leave a domain row")
}
