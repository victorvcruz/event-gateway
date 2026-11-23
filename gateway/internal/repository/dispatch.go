package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DispatchRow struct {
	EventID       uuid.UUID
	ServiceID     string
	CallbackURL   string
	EventType     string
	CorrelationID string
	Payload       json.RawMessage
	OccurredAt    time.Time
}

type Dispatch struct {
	pool *pgxpool.Pool
}

func NewDispatch(pool *pgxpool.Pool) *Dispatch {
	return &Dispatch{pool: pool}
}

// InsertPending insere a linha em dispatch_outbox com ON CONFLICT (event_id,
// service_id) DO NOTHING. Devolve inserted=true se criou e false se a PK já
// existia, sem erro nos dois casos. É isso que absorve a reentrega do Kafka.
func (d *Dispatch) InsertPending(ctx context.Context, tx pgx.Tx, row DispatchRow) (inserted bool, err error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO dispatch_outbox
			(event_id, service_id, callback_url, event_type, correlation_id,
			 payload, occurred_at, status, retry_count, next_attempt_at)
		VALUES
			($1, $2, $3, $4, $5, $6, $7, 'PENDING', 0, now())
		ON CONFLICT (event_id, service_id) DO NOTHING
	`, row.EventID, row.ServiceID, row.CallbackURL, row.EventType, row.CorrelationID,
		[]byte(row.Payload), row.OccurredAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// MarkSent muda a linha para SENT e grava sent_at. A chave é (event_id,
// service_id), e o service_id vem do wrapper DispatchEnvelope.
func (d *Dispatch) MarkSent(ctx context.Context, tx pgx.Tx,
	eventID uuid.UUID, serviceID string, sentAt time.Time,
) error {
	_, err := tx.Exec(ctx, `
		UPDATE dispatch_outbox SET status='SENT', sent_at=$3
		WHERE event_id=$1 AND service_id=$2
	`, eventID, serviceID, sentAt)
	return err
}

// MarkDead muda a linha para DEAD e grava o retry_count final.
func (d *Dispatch) MarkDead(ctx context.Context, tx pgx.Tx,
	eventID uuid.UUID, serviceID string, retryCount int,
) error {
	_, err := tx.Exec(ctx, `
		UPDATE dispatch_outbox SET status='DEAD', retry_count=$3
		WHERE event_id=$1 AND service_id=$2
	`, eventID, serviceID, retryCount)
	return err
}

// RecordDead também grava na linha o motivo da falha definitiva.
func (d *Dispatch) RecordDead(ctx context.Context, tx pgx.Tx,
	eventID uuid.UUID, serviceID string, retryCount int, lastError string,
) error {
	_, err := tx.Exec(ctx, `
		UPDATE dispatch_outbox
		SET status='DEAD', retry_count=$3, last_error=$4
		WHERE event_id=$1 AND service_id=$2
	`, eventID, serviceID, retryCount, lastError)
	return err
}

// AppendEventLogDispatched grava event_log(DISPATCHED, service_id) dentro da
// tx. Complementa o AppendReceived da ingestão.
func (d *Dispatch) AppendEventLogDispatched(ctx context.Context, tx pgx.Tx,
	eventID uuid.UUID, serviceID, eventType, correlationID string,
) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO event_log (event_id, correlation_id, event_type, direction, service_id)
		VALUES ($1, $2, $3, 'DISPATCHED', $4)
	`, eventID, correlationID, eventType, serviceID)
	return err
}
