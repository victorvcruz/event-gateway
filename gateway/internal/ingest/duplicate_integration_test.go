package ingest_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/victorvcruz/event-gateway/gateway/internal/ingest"
	"github.com/victorvcruz/event-gateway/gateway/internal/repository"
	gatewaymigrations "github.com/victorvcruz/event-gateway/gateway/migrations"
	"github.com/victorvcruz/event-gateway/shared/outbox"
	outboxmig "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
	"github.com/victorvcruz/event-gateway/shared/testpostgres"
)

// TestDuplicateEventIDWithDifferentContentIsTreatedAsDuplicate documenta o
// comportamento da PoC: a deduplicação da ingestão usa apenas event_id. Uma
// segunda submissão com o mesmo event_id e tipo, correlação ou payload
// diferentes recebe o mesmo desfecho de duplicata (409) e NÃO substitui o
// envelope aceito originalmente; o conteúdo não é comparado.
func TestDuplicateEventIDWithDifferentContentIsTreatedAsDuplicate(t *testing.T) {
	harness := testpostgres.Start(t)
	harness.Apply(t,
		func(dsn string) error { return outboxmig.Apply(dsn) },
		func(dsn string) error { return outboxmig.Apply(dsn, outboxmig.WithTableName("dispatch_outbox")) },
		gatewaymigrations.Apply,
	)
	svc := ingest.NewService(harness.Pool, outbox.NewClient(harness.Pool, outbox.DefaultTable),
		repository.NewEventLog(harness.Pool), fakeValidator{})
	ctx := context.Background()

	eventID := uuid.NewString()
	original, originalRaw := envelope(t, map[string]any{"event_id": eventID, "correlation_id": "saga-original", "payload": map[string]any{"n": 1}})
	_, outcome, err := svc.Handle(ctx, original, originalRaw)
	require.NoError(t, err)
	require.Equal(t, ingest.OutcomeAccepted, outcome)

	for name, override := range map[string]map[string]any{
		"different payload":     {"event_id": eventID, "correlation_id": "saga-original", "payload": map[string]any{"n": 2}},
		"different correlation": {"event_id": eventID, "correlation_id": "saga-other", "payload": map[string]any{"n": 1}},
		"different type":        {"event_id": eventID, "event_type": "order.cancelled", "correlation_id": "saga-original", "payload": map[string]any{"n": 1}},
	} {
		t.Run(name, func(t *testing.T) {
			env, raw := envelope(t, override)
			res, outcome, err := svc.Handle(ctx, env, raw)
			require.NoError(t, err)
			assert.Equal(t, ingest.OutcomeDuplicate, outcome)
			assert.Equal(t, "PENDING", res.ExistingStatus)
		})
	}

	var storedType, storedCorrelation string
	var storedPayload json.RawMessage
	require.NoError(t, harness.Pool.QueryRow(ctx,
		`SELECT event_type, correlation_id, payload FROM outbox WHERE event_id=$1`, eventID).
		Scan(&storedType, &storedCorrelation, &storedPayload))
	assert.Equal(t, "order.created", storedType)
	assert.Equal(t, "saga-original", storedCorrelation)
	assert.JSONEq(t, string(originalRaw), string(storedPayload), "envelope original preservado")

	var received int
	require.NoError(t, harness.Pool.QueryRow(ctx,
		`SELECT count(*) FROM event_log WHERE event_id=$1 AND direction='RECEIVED'`, eventID).Scan(&received))
	assert.Equal(t, 1, received)
}

// TestSchemaValidationRunsBeforeDeduplication documenta a ordem do caminho de
// ingestão: o schema vigente é verificado antes da deduplicação. Uma reentrega
// de um evento já aceito que deixou de validar (por mudança incompatível de
// schema) recebe violação de schema (422), e não o desfecho de duplicata.
func TestSchemaValidationRunsBeforeDeduplication(t *testing.T) {
	env, raw := envelope(t, nil)
	outboxFake := &fakeOutbox{insertErr: outbox.ErrDuplicate, lookupStatus: "SENT"}
	svc := ingest.NewService(fakeBeginner{tx: noopTx{}}, outboxFake, &fakeLog{},
		fakeValidator{err: errors.New("does not validate against latest schema")})

	_, outcome, err := svc.Handle(context.Background(), env, raw)
	require.Error(t, err)
	assert.Equal(t, ingest.OutcomeSchemaViolation, outcome)
	assert.Equal(t, 0, outboxFake.insertCalled, "deduplicação não é alcançada")
}
