// Package delivery reúne o que o Worker do outbox aciona quando um evento
// termina: o sink de dead_letter, o hook de sucesso e o sender HTTP do dispatch.
package delivery

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/victorvcruz/event-gateway/gateway/internal/repository"
	"github.com/victorvcruz/event-gateway/shared/outbox"
)

// ServiceIDKafkaBroker identifica o gateway como origem no dead_letter quando
// a falha foi na publicação no Kafka.
const ServiceIDKafkaBroker = "kafka-broker"

// DeadLetterSink é chamado pelo Worker quando retry_count chega a MaxRetries
// ou o Sender devolve ErrNonRetryable. Para saber de qual fluxo veio a linha,
// tenta decodificar o wrapper DispatchEnvelope de row.Payload. Se der certo, é
// dispatch: usa o service_id do wrapper e atualiza dispatch_outbox pela PK
// composta. Se não, é o relay do Kafka: usa o serviceID do construtor e
// atualiza outbox.
//
// Roda sempre na tx do Worker, para não travar com o SELECT ... FOR UPDATE
// SKIP LOCKED.
type DeadLetterSink struct {
	outboxClient   *outbox.Client
	dispatch       *repository.Dispatch
	deadLetter     *repository.DeadLetter
	relayMetric    *prometheus.CounterVec
	dispatchMetric *prometheus.CounterVec
	serviceID      string
}

func NewDeadLetterSink(
	client *outbox.Client,
	repo *repository.DeadLetter,
	metric *prometheus.CounterVec,
	serviceID string,
) *DeadLetterSink {
	if serviceID == "" {
		serviceID = ServiceIDKafkaBroker
	}
	return &DeadLetterSink{
		outboxClient: client,
		deadLetter:   repo,
		relayMetric:  metric,
		serviceID:    serviceID,
	}
}

// WithDispatch habilita o modo dispatch. Quando o wrapper decodifica, o sink
// atualiza dispatch_outbox e incrementa dispatchMetric, com label service_id.
func (s *DeadLetterSink) WithDispatch(
	dispatch *repository.Dispatch,
	metric *prometheus.CounterVec,
) *DeadLetterSink {
	s.dispatch = dispatch
	s.dispatchMetric = metric
	return s
}

// Handle satisfaz outbox.DeadLetterHook.
func (s *DeadLetterSink) Handle(ctx context.Context, tx pgx.Tx, row outbox.Row, cause error) error {
	env, err := DecodeDispatchEnvelope(row.Payload)
	if err == nil && s.dispatch != nil {
		return s.handleDispatch(ctx, tx, row, env, cause)
	}
	return s.handleRelay(ctx, tx, row, cause)
}

// handleRelay trata o relay do Kafka: atualiza outbox e usa o service_id do construtor.
func (s *DeadLetterSink) handleRelay(ctx context.Context, tx pgx.Tx, row outbox.Row, cause error) error {
	if err := s.outboxClient.RecordDead(ctx, tx, row.EventID, row.RetryCount, cause); err != nil {
		return err
	}
	failureReason := ""
	if cause != nil {
		failureReason = cause.Error()
	}
	if err := s.deadLetter.Insert(ctx, tx, repository.DeadLetterRow{
		EventID:       row.EventID,
		EventType:     row.EventType,
		ServiceID:     s.serviceID,
		Payload:       row.Payload,
		FailureReason: failureReason,
	}); err != nil {
		return err
	}
	if s.relayMetric != nil {
		s.relayMetric.WithLabelValues(row.EventType).Inc()
	}
	return nil
}

// handleDispatch trata o dispatch HTTP: atualiza dispatch_outbox pela PK
// composta, usa o service_id do wrapper e grava em dead_letter o envelope cru,
// sem o wrapper.
func (s *DeadLetterSink) handleDispatch(ctx context.Context, tx pgx.Tx,
	row outbox.Row, env DispatchEnvelope, cause error,
) error {
	failureReason := ""
	if cause != nil {
		failureReason = cause.Error()
	}
	if err := s.dispatch.RecordDead(ctx, tx, row.EventID, env.Target.ServiceID, row.RetryCount, failureReason); err != nil {
		return err
	}
	if err := s.deadLetter.Insert(ctx, tx, repository.DeadLetterRow{
		EventID:       row.EventID,
		EventType:     row.EventType,
		ServiceID:     env.Target.ServiceID,
		Payload:       json.RawMessage(env.Envelope),
		FailureReason: failureReason,
	}); err != nil {
		return err
	}
	if s.dispatchMetric != nil {
		s.dispatchMetric.WithLabelValues(row.EventType, env.Target.ServiceID).Inc()
	}
	return nil
}
