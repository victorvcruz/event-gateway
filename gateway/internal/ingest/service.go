// Package ingest orquestra o POST /events: valida o envelope e o schema e
// grava outbox + event_log(RECEIVED) na mesma transação.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
	"github.com/victorvcruz/event-gateway/shared/outbox"
)

// Implementações concretas: shared/outbox.Client, repository.EventLog e
// schema.Loader.

type Validator interface {
	Validate(ctx context.Context, eventType string, envelope json.RawMessage) error
}

type OutboxClient interface {
	InsertPending(ctx context.Context, tx pgx.Tx, row outbox.Row) error
	// LookupStatusPool consulta o status fora de transação. Precisa disso
	// depois de um ErrDuplicate, porque o Postgres aborta a tx corrente.
	LookupStatusPool(ctx context.Context, eventID uuid.UUID) (string, error)
}

type EventLogRepository interface {
	AppendReceived(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, correlationID, eventType string) error
}

type TxBeginner interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// Result diz ao handler HTTP o que aconteceu, para responder 202 ou 409 sem
// consultar o banco de novo.
type Result struct {
	EventID        uuid.UUID
	ExistingStatus string // preenchido em Duplicate
}

// Outcome é o resultado do Handle.
type Outcome int

const (
	OutcomeAccepted Outcome = iota
	OutcomeDuplicate
	OutcomeSchemaViolation
	OutcomeSchemaNotFound
	OutcomeRegistryUnavailable
	OutcomeInternal
)

type ServiceError struct {
	Outcome Outcome
	Cause   error
}

func (e *ServiceError) Error() string {
	if e.Cause == nil {
		return fmt.Sprintf("ingest outcome %d", e.Outcome)
	}
	return e.Cause.Error()
}

func (e *ServiceError) Unwrap() error { return e.Cause }

type Service struct {
	tx        TxBeginner
	outbox    OutboxClient
	eventLog  EventLogRepository
	validator Validator
}

func NewService(tx TxBeginner, outboxClient OutboxClient, log EventLogRepository, v Validator) *Service {
	return &Service{tx: tx, outbox: outboxClient, eventLog: log, validator: v}
}

// Handle valida o schema, insere em outbox, insere no event_log e commita,
// nessa ordem.
func (s *Service) Handle(ctx context.Context, env Envelope, raw json.RawMessage) (Result, Outcome, error) {
	if err := env.Validate(); err != nil {
		return Result{}, OutcomeSchemaViolation, err
	}

	if err := s.validator.Validate(ctx, env.EventType, raw); err != nil {
		if errors.Is(err, schema.ErrRegistryUnavailable) {
			return Result{}, OutcomeRegistryUnavailable, err
		}
		if errors.Is(err, schema.ErrSchemaNotFound) {
			return Result{}, OutcomeSchemaNotFound, err
		}
		return Result{}, OutcomeSchemaViolation, err
	}

	eventID, err := uuid.Parse(env.EventID)
	if err != nil {
		return Result{}, OutcomeSchemaViolation, err
	}

	tx, err := s.tx.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Result{}, OutcomeInternal, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// outbox.payload guarda o envelope cru inteiro, não só env.Payload: o
	// Relay o publica direto no Kafka e o consumer precisa dele completo para
	// ler event_id, event_type e correlation_id antes do fan-out.
	row := outbox.Row{
		EventID:       eventID,
		EventType:     env.EventType,
		CorrelationID: env.CorrelationID,
		Payload:       raw,
	}
	if err := s.outbox.InsertPending(ctx, tx, row); err != nil {
		if errors.Is(err, outbox.ErrDuplicate) {
			_ = tx.Rollback(ctx)
			status, lookupErr := s.outbox.LookupStatusPool(ctx, eventID)
			if lookupErr != nil {
				return Result{EventID: eventID}, OutcomeInternal, lookupErr
			}
			return Result{EventID: eventID, ExistingStatus: status}, OutcomeDuplicate, nil
		}
		return Result{}, OutcomeInternal, fmt.Errorf("insert outbox: %w", err)
	}

	if err := s.eventLog.AppendReceived(ctx, tx, eventID, env.CorrelationID, env.EventType); err != nil {
		return Result{}, OutcomeInternal, fmt.Errorf("append event_log: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Result{}, OutcomeInternal, fmt.Errorf("commit: %w", err)
	}
	return Result{EventID: eventID}, OutcomeAccepted, nil
}
