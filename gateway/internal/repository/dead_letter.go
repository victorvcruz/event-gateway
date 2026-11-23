package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DeadLetterRow é uma linha da tabela dead_letter. O service_id é
// "kafka-broker" quando a falha foi na publicação do gateway no Kafka; nos
// dispatchers de serviço, é o id do serviço.
type DeadLetterRow struct {
	EventID       uuid.UUID
	EventType     string
	ServiceID     string
	Payload       json.RawMessage
	FailureReason string
}

type KafkaInvalidRow struct {
	EventID         uuid.UUID
	EventType       string
	RawPayload      []byte
	FailureCode     string
	FailureReason   string
	SourceTopic     string
	SourcePartition int32
	SourceOffset    int64
}

const failureReasonMaxLen = 4000

type DeadLetter struct {
	pool *pgxpool.Pool
}

func NewDeadLetter(pool *pgxpool.Pool) *DeadLetter {
	return &DeadLetter{pool: pool}
}

// Insert grava a linha em dead_letter na tx corrente. O failure_reason é
// truncado em failureReasonMaxLen bytes, porque às vezes chega um stack trace
// gigante.
func (d *DeadLetter) Insert(ctx context.Context, tx pgx.Tx, row DeadLetterRow) error {
	reason := row.FailureReason
	if len(reason) > failureReasonMaxLen {
		reason = reason[:failureReasonMaxLen]
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO dead_letter (event_id, event_type, service_id, payload, failure_reason)
		VALUES ($1, $2, $3, $4, $5)
	`, row.EventID, row.EventType, row.ServiceID, []byte(row.Payload), reason)
	return err
}

var kafkaInvalidNamespace = uuid.MustParse("236eac44-9ec9-5f9e-b59f-727e488c6143")

func KafkaInvalidEventID(topic string, partition int32, offset int64) uuid.UUID {
	return uuid.NewSHA1(kafkaInvalidNamespace, []byte(fmt.Sprintf("%s:%d:%d", topic, partition, offset)))
}

func (d *DeadLetter) InsertKafkaInvalid(ctx context.Context, tx pgx.Tx, row KafkaInvalidRow) (bool, error) {
	reason := row.FailureReason
	if len(reason) > failureReasonMaxLen {
		reason = reason[:failureReasonMaxLen]
	}
	if row.EventID == uuid.Nil {
		row.EventID = KafkaInvalidEventID(row.SourceTopic, row.SourcePartition, row.SourceOffset)
	}
	var payload any
	if json.Valid(row.RawPayload) {
		payload = string(row.RawPayload)
	}
	result, err := tx.Exec(ctx, `
		INSERT INTO dead_letter (
			event_id, event_type, service_id, payload, raw_payload, failure_reason,
			failure_code, source_topic, source_partition, source_offset
		)
		VALUES ($1, $2, 'gateway-consumer', $3::jsonb, $4, $5, $6, $7, $8, $9)
		ON CONFLICT DO NOTHING
	`, row.EventID, row.EventType, payload, row.RawPayload, reason, row.FailureCode,
		row.SourceTopic, row.SourcePartition, row.SourceOffset)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, nil
}
