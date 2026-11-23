package ingest_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/victorvcruz/event-gateway/gateway/internal/ingest"
	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
	"github.com/victorvcruz/event-gateway/shared/outbox"
)

// noopTx implementa pgx.Tx pelo mínimo que Service.Handle exercita
// (Commit/Rollback). Os outros métodos existem via embed nil — não devem
// ser chamados no fluxo.
type noopTx struct{ pgx.Tx }

func (noopTx) Commit(_ context.Context) error   { return nil }
func (noopTx) Rollback(_ context.Context) error { return nil }

type fakeBeginner struct {
	tx  pgx.Tx
	err error
}

func (f fakeBeginner) BeginTx(_ context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	return f.tx, f.err
}

type fakeOutbox struct {
	insertErr    error
	insertCalled int
	lookupStatus string
}

func (f *fakeOutbox) InsertPending(_ context.Context, _ pgx.Tx, _ outbox.Row) error {
	f.insertCalled++
	return f.insertErr
}

func (f *fakeOutbox) LookupStatusPool(_ context.Context, _ uuid.UUID) (string, error) {
	return f.lookupStatus, nil
}

type fakeLog struct{ appendCalled int }

func (f *fakeLog) AppendReceived(_ context.Context, _ pgx.Tx, _ uuid.UUID, _, _ string) error {
	f.appendCalled++
	return nil
}

type fakeValidator struct{ err error }

func (f fakeValidator) Validate(_ context.Context, _ string, _ json.RawMessage) error {
	return f.err
}

func envelope(t *testing.T, override map[string]any) (ingest.Envelope, json.RawMessage) {
	t.Helper()
	base := map[string]any{
		"event_id":       uuid.NewString(),
		"event_type":     "order.created",
		"correlation_id": "saga-1",
		"timestamp":      time.Now().UTC().Format(time.RFC3339Nano),
		"payload":        map[string]any{"n": 1},
	}
	for k, v := range override {
		base[k] = v
	}
	raw, err := json.Marshal(base)
	require.NoError(t, err)

	var env ingest.Envelope
	require.NoError(t, json.Unmarshal(raw, &env))
	return env, raw
}

func TestServiceAccepts(t *testing.T) {
	env, raw := envelope(t, nil)
	outbox := &fakeOutbox{}
	log := &fakeLog{}
	svc := ingest.NewService(fakeBeginner{tx: noopTx{}}, outbox, log, fakeValidator{})

	res, outcome, err := svc.Handle(context.Background(), env, raw)
	require.NoError(t, err)
	assert.Equal(t, ingest.OutcomeAccepted, outcome)
	assert.NotEqual(t, uuid.Nil, res.EventID)
	assert.Equal(t, 1, outbox.insertCalled)
	assert.Equal(t, 1, log.appendCalled)
}

func TestServiceDuplicateReturnsExistingStatus(t *testing.T) {
	env, raw := envelope(t, nil)
	outboxFake := &fakeOutbox{insertErr: outbox.ErrDuplicate, lookupStatus: "PENDING"}
	log := &fakeLog{}
	svc := ingest.NewService(fakeBeginner{tx: noopTx{}}, outboxFake, log, fakeValidator{})

	res, outcome, err := svc.Handle(context.Background(), env, raw)
	require.NoError(t, err)
	assert.Equal(t, ingest.OutcomeDuplicate, outcome)
	assert.Equal(t, "PENDING", res.ExistingStatus)
	assert.Equal(t, 0, log.appendCalled, "event_log NÃO recebe linha no caminho duplicado")
}

func TestServiceSchemaViolation(t *testing.T) {
	env, raw := envelope(t, nil)
	svc := ingest.NewService(fakeBeginner{tx: noopTx{}}, &fakeOutbox{}, &fakeLog{},
		fakeValidator{err: errors.New("does not validate")})

	_, outcome, err := svc.Handle(context.Background(), env, raw)
	require.Error(t, err)
	assert.Equal(t, ingest.OutcomeSchemaViolation, outcome)
}

func TestServiceSchemaNotFound(t *testing.T) {
	env, raw := envelope(t, nil)
	svc := ingest.NewService(fakeBeginner{tx: noopTx{}}, &fakeOutbox{}, &fakeLog{},
		fakeValidator{err: schema.ErrSchemaNotFound})

	_, outcome, err := svc.Handle(context.Background(), env, raw)
	require.Error(t, err)
	assert.Equal(t, ingest.OutcomeSchemaNotFound, outcome)
	assert.ErrorIs(t, err, schema.ErrSchemaNotFound)
}

func TestServiceRegistryUnavailable(t *testing.T) {
	env, raw := envelope(t, nil)
	svc := ingest.NewService(fakeBeginner{tx: noopTx{}}, &fakeOutbox{}, &fakeLog{},
		fakeValidator{err: schema.ErrRegistryUnavailable})

	_, outcome, err := svc.Handle(context.Background(), env, raw)
	require.Error(t, err)
	assert.Equal(t, ingest.OutcomeRegistryUnavailable, outcome)
	assert.ErrorIs(t, err, schema.ErrRegistryUnavailable)
}

func TestServiceEnvelopeMissingField(t *testing.T) {
	env, raw := envelope(t, map[string]any{"event_id": ""})
	svc := ingest.NewService(fakeBeginner{tx: noopTx{}}, &fakeOutbox{}, &fakeLog{}, fakeValidator{})

	_, outcome, err := svc.Handle(context.Background(), env, raw)
	require.Error(t, err)
	assert.Equal(t, ingest.OutcomeSchemaViolation, outcome)
}

func TestServiceDatabaseFailureIsInternal(t *testing.T) {
	env, raw := envelope(t, nil)
	svc := ingest.NewService(fakeBeginner{err: errors.New("database unavailable")}, &fakeOutbox{}, &fakeLog{}, fakeValidator{})

	_, outcome, err := svc.Handle(context.Background(), env, raw)
	require.Error(t, err)
	assert.Equal(t, ingest.OutcomeInternal, outcome)
	assert.Contains(t, err.Error(), "begin tx")
}
