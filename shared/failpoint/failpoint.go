// Package failpoint expõe pontos de crash determinísticos para a suíte de
// chaos local. Os builds normais usam uma versão que não faz nada, escolhida
// por build tag.
package failpoint

import (
	"context"
	"net/http"

	"go.uber.org/zap"
)

const (
	OrderHTTPAfterCommit           = "order.http.after_commit"
	GatewayHTTPAfterIngestCommit   = "gateway.http.after_ingest_commit"
	DispatchAfterHandlerCommit     = "dispatch.after_handler_commit"
	OutboxAfterSendACK             = "outbox.after_send_ack"
	KafkaConsumerAfterDBCommit     = "kafka.consumer.after_db_commit"
	PaymentCompensationPreCommit   = "payment.compensation.before_commit"
	InventoryCompensationPreCommit = "inventory.compensation.before_commit"
)

// Checkpoint identifica de forma estável uma janela de crash.
type Checkpoint struct {
	Point         string `json:"point"`
	EventID       string `json:"event_id,omitempty"`
	EventType     string `json:"event_type,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	OutboxTable   string `json:"outbox_table,omitempty"`
	DeliveryKey   string `json:"delivery_key,omitempty"`
}

// Probe é tudo que o código precisa para marcar um ponto de crash.
type Probe interface {
	Hit(context.Context, Checkpoint)
}

// New escolhe a implementação conforme o build. Nos builds normais devolve um
// Probe que não faz nada; nos de chaos usa CHAOS_CONTROLLER_URL.
func New(processID string, log *zap.Logger) Probe {
	if log == nil {
		log = zap.NewNop()
	}
	return newRuntime(processID, log)
}

type contextKey struct{}

// WithContext coloca o Probe no ctx, para os handlers o usarem sem mudar suas
// assinaturas.
func WithContext(ctx context.Context, probe Probe) context.Context {
	if probe == nil {
		return ctx
	}
	return context.WithValue(ctx, contextKey{}, probe)
}

// Hit chama o Probe do ctx. Se não houver, não faz nada.
func Hit(ctx context.Context, checkpoint Checkpoint) {
	if probe, ok := ctx.Value(contextKey{}).(Probe); ok && probe != nil {
		probe.Hit(ctx, checkpoint)
	}
}

// Middleware coloca o Probe no contexto de cada requisição.
func Middleware(probe Probe, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(WithContext(r.Context(), probe)))
	})
}
