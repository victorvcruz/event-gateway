package repository

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EventLog struct {
	pool *pgxpool.Pool
}

func NewEventLog(pool *pgxpool.Pool) *EventLog {
	return &EventLog{pool: pool}
}

func (e *EventLog) AppendReceived(ctx context.Context, tx pgx.Tx,
	eventID uuid.UUID, correlationID, eventType string,
) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO event_log (event_id, correlation_id, event_type, direction, service_id)
		VALUES ($1, $2, $3, 'RECEIVED', NULL)
	`, eventID, correlationID, eventType)
	return err
}
