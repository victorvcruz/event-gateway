package repository_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/victorvcruz/event-gateway/gateway/internal/repository"
)

func newDispatchRow(t *testing.T, serviceID string) repository.DispatchRow {
	t.Helper()
	return repository.DispatchRow{
		EventID:       uuid.New(),
		ServiceID:     serviceID,
		CallbackURL:   "http://" + serviceID + "/dispatch",
		EventType:     "order.created",
		CorrelationID: "saga-x",
		Payload:       json.RawMessage(`{"target":{"service_id":"` + serviceID + `","callback_url":"http://x"},"envelope":{"a":1}}`),
		OccurredAt:    time.Now().UTC(),
	}
}

func TestDispatchInsertPendingReturnsInsertedTrueOnFirstCall(t *testing.T) {
	pool, teardown := startAndMigrate(t)
	defer teardown()

	dispatch := repository.NewDispatch(pool)
	row := newDispatchRow(t, "payment-service")

	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()

	inserted, err := dispatch.InsertPending(ctx, tx, row)
	require.NoError(t, err)
	assert.True(t, inserted, "primeira inserção retorna true")
	require.NoError(t, tx.Commit(ctx))

	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM dispatch_outbox WHERE event_id=$1`, row.EventID).Scan(&n))
	assert.Equal(t, 1, n)
}

func TestDispatchInsertPendingDedupesOnCompositePK(t *testing.T) {
	pool, teardown := startAndMigrate(t)
	defer teardown()

	dispatch := repository.NewDispatch(pool)
	row := newDispatchRow(t, "payment-service")

	ctx := context.Background()

	// primeiro INSERT — inserted=true
	tx1, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	inserted1, err := dispatch.InsertPending(ctx, tx1, row)
	require.NoError(t, err)
	require.NoError(t, tx1.Commit(ctx))
	assert.True(t, inserted1)

	// segundo INSERT com MESMO (event_id, service_id) — inserted=false, sem erro
	tx2, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	inserted2, err := dispatch.InsertPending(ctx, tx2, row)
	require.NoError(t, err, "ON CONFLICT DO NOTHING NÃO deve retornar erro")
	assert.False(t, inserted2, "segunda inserção retorna false (linha já existia)")
	require.NoError(t, tx2.Commit(ctx))

	// contagem final = 1 (nenhuma duplicata)
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM dispatch_outbox WHERE event_id=$1`, row.EventID).Scan(&n))
	assert.Equal(t, 1, n, "constraint composta impede duplicata")
}

func TestDispatchInsertPendingAllowsSameEventIDDifferentServiceID(t *testing.T) {
	pool, teardown := startAndMigrate(t)
	defer teardown()

	dispatch := repository.NewDispatch(pool)
	base := newDispatchRow(t, "payment-service")

	// mesmo event_id, service_id distinto — deve inserir (fan-out normal)
	fanoutRow := base
	fanoutRow.ServiceID = "inventory-service"
	fanoutRow.CallbackURL = "http://inventory/dispatch"

	ctx := context.Background()

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	inserted, err := dispatch.InsertPending(ctx, tx, base)
	require.NoError(t, err)
	assert.True(t, inserted)
	inserted, err = dispatch.InsertPending(ctx, tx, fanoutRow)
	require.NoError(t, err)
	assert.True(t, inserted, "mesmo event_id + service_id distinto → inserted=true")
	require.NoError(t, tx.Commit(ctx))

	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM dispatch_outbox WHERE event_id=$1`, base.EventID).Scan(&n))
	assert.Equal(t, 2, n, "fan-out para 2 subscribers → 2 linhas independentes")
}

func TestDispatchMarkSentUpdatesStatusAndSentAt(t *testing.T) {
	pool, teardown := startAndMigrate(t)
	defer teardown()

	dispatch := repository.NewDispatch(pool)
	row := newDispatchRow(t, "payment-service")

	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	_, err = dispatch.InsertPending(ctx, tx, row)
	require.NoError(t, err)
	sentAt := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, dispatch.MarkSent(ctx, tx, row.EventID, row.ServiceID, sentAt))
	require.NoError(t, tx.Commit(ctx))

	var status string
	var storedSentAt *time.Time
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT status, sent_at FROM dispatch_outbox
		WHERE event_id=$1 AND service_id=$2`, row.EventID, row.ServiceID,
	).Scan(&status, &storedSentAt))
	assert.Equal(t, "SENT", status)
	require.NotNil(t, storedSentAt)
	assert.WithinDuration(t, sentAt, *storedSentAt, time.Second)
}
