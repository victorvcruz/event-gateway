package delivery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/victorvcruz/event-gateway/gateway/internal/repository"
	"github.com/victorvcruz/event-gateway/shared/outbox"
)

// SuccessHookEventLog é o outbox.SuccessHook do dispatch. Na mesma tx que
// marca a linha como SENT, grava event_log(DISPATCHED) para o subscriber. O
// service_id vem do wrapper DispatchEnvelope em row.Payload.
type SuccessHookEventLog struct {
	dispatch *repository.Dispatch
	latency  *prometheus.HistogramVec
	clock    func() time.Time
	origins  map[[16]byte]time.Time
}

func NewSuccessHookEventLog(
	dispatch *repository.Dispatch,
	latency *prometheus.HistogramVec,
) *SuccessHookEventLog {
	return &SuccessHookEventLog{
		dispatch: dispatch,
		latency:  latency,
		clock:    time.Now,
		origins:  make(map[[16]byte]time.Time),
	}
}

// Handle satisfaz outbox.SuccessHook. Na ordem: decodifica o wrapper (se
// falhar, o erro faz o Worker dar rollback), insere o event_log DISPATCHED,
// marca dispatch_outbox como SENT com sent_at e observa o histograma de
// latência. Tudo roda na tx do Worker, então um rollback desfaz tudo.
func (h *SuccessHookEventLog) Handle(ctx context.Context, tx pgx.Tx, row outbox.Row) error {
	env, err := DecodeDispatchEnvelope(row.Payload)
	if err != nil {
		return fmt.Errorf("success hook: %w", err)
	}
	if env.Target.ServiceID == "" {
		return errors.New("success hook: empty service_id in wrapper")
	}

	now := h.clock()
	if err := h.dispatch.AppendEventLogDispatched(ctx, tx,
		row.EventID, env.Target.ServiceID, row.EventType, row.CorrelationID,
	); err != nil {
		return fmt.Errorf("append event_log: %w", err)
	}
	if err := h.dispatch.MarkSent(ctx, tx,
		row.EventID, env.Target.ServiceID, now,
	); err != nil {
		return fmt.Errorf("mark dispatch sent: %w", err)
	}

	if h.latency != nil {
		if created, ok := h.dispatchCreatedAt(ctx, tx, row.EventID, env.Target.ServiceID); ok {
			h.latency.WithLabelValues(row.EventType, env.Target.ServiceID).
				Observe(now.Sub(created).Seconds())
		}
	}
	return nil
}

// dispatchCreatedAt lê o created_at da linha, que o FOR UPDATE SKIP LOCKED do
// Worker já travou. Se a leitura falhar, só pula a métrica de latência, para
// não perder o UPDATE.
func (h *SuccessHookEventLog) dispatchCreatedAt(ctx context.Context, tx pgx.Tx,
	eventID [16]byte, serviceID string,
) (time.Time, bool) {
	var created time.Time
	err := tx.QueryRow(ctx,
		`SELECT created_at FROM dispatch_outbox WHERE event_id=$1 AND service_id=$2`,
		eventID, serviceID,
	).Scan(&created)
	if err != nil {
		return time.Time{}, false
	}
	return created, true
}
