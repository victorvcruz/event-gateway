// Package dispatcher é o SDK Go para os serviços de negócio receberem o
// POST /dispatch do gateway e roteá-lo por event_type.
//
// Uso típico no bootstrap de um serviço:
//
//	d := dispatcher.New(dispatcher.WithLogger(logger))
//	d.Handle("order.created", paymentSvc.HandleOrderCreated)
//	d.Handle("payment.failed", orderSvc.HandlePaymentFailed)
//	http.Handle("/dispatch", d)
//	http.ListenAndServe(":8080", nil)
package dispatcher

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/gateway/pkg/client"
)

// Event reaproveita o tipo de pkg/client, então uma mudança no formato do
// envelope chega aos handlers sem trabalho extra.
type Event = client.Event

// Handler é a função que um serviço registra por event_type. O ctx carrega o
// correlation_id do header X-Correlation-Id, acessível por
// CorrelationIDFromContext.
type Handler func(ctx context.Context, evt Event) error

// Erros que o handler pode devolver (com fmt.Errorf e %w) para escolher a
// resposta HTTP:
var (
	// ErrDuplicate vira HTTP 409, que o gateway trata como sucesso (SENT).
	ErrDuplicate = errors.New("dispatcher: already processed")
	// ErrBadRequest vira HTTP 400, e o gateway marca a linha como DEAD na hora.
	ErrBadRequest = errors.New("dispatcher: bad payload")
)

// contextKey é privado; o handler usa CorrelationIDFromContext.
type contextKey struct{ name string }

var correlationIDKey = contextKey{"correlation_id"}

const DispatchTokenHeader = "X-Gateway-Token"

// Authenticate exige o token de dispatch compartilhado. Token vazio na config é
// erro do servidor e mantém o endpoint fechado.
func Authenticate(token string, next http.Handler) http.Handler {
	if token == "" {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		provided := r.Header.Get(DispatchTokenHeader)
		if len(provided) != len(token) || subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// CorrelationIDFromContext devolve o X-Correlation-Id que o Dispatcher pôs no
// ctx durante o ServeHTTP. Fora desse fluxo, devolve string vazia.
func CorrelationIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(correlationIDKey).(string); ok {
		return v
	}
	return ""
}

// Dispatcher é um http.Handler que decodifica o envelope e roteia por
// event_type. Registre os handlers no boot, antes de expor o servidor.
type Dispatcher struct {
	log         *zap.Logger
	afterHandle func(context.Context, Event)
	mu          sync.RWMutex
	handlers    map[string]Handler
}

// New cria um Dispatcher sem handlers.
func New(opts ...Option) *Dispatcher {
	o := options{log: zap.NewNop()}
	for _, apply := range opts {
		apply(&o)
	}
	return &Dispatcher{
		log:         o.log,
		afterHandle: o.afterHandle,
		handlers:    make(map[string]Handler),
	}
}

// Handle registra h para o eventType. Chame no boot, antes de expor o
// Dispatcher. Registrar o mesmo eventType de novo substitui o handler anterior.
func (d *Dispatcher) Handle(eventType string, h Handler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.handlers[eventType] = h
}

// ServeHTTP implementa http.Handler e responde assim:
//
//	decode falha                                  → 400
//	event_type sem handler                        → 200 (o gateway segue em frente)
//	handler retorna nil                           → 200
//	handler retorna erro com ErrDuplicate         → 409
//	handler retorna erro com ErrBadRequest        → 400
//	handler retorna qualquer outro erro           → 500
func (d *Dispatcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		d.log.Warn("read body failed", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var evt Event
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&evt); err != nil {
		d.log.Warn("decode envelope failed", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		d.log.Warn("decode envelope failed", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := validateEnvelope(evt); err != nil {
		d.log.Warn("invalid envelope", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if header := r.Header.Get("X-Correlation-Id"); header != "" && header != evt.CorrelationID {
		d.log.Warn("correlation header mismatch")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	d.mu.RLock()
	h, ok := d.handlers[evt.EventType]
	d.mu.RUnlock()
	if !ok {
		// event_type que este serviço não trata: responde 200. Ficar em retry
		// por causa de um handler que não existe só travaria o gateway.
		w.WriteHeader(http.StatusOK)
		return
	}

	ctx := context.WithValue(r.Context(), correlationIDKey, evt.CorrelationID)

	if err := h(ctx, evt); err != nil {
		switch {
		case errors.Is(err, ErrDuplicate):
			w.WriteHeader(http.StatusConflict)
		case errors.Is(err, ErrBadRequest):
			w.WriteHeader(http.StatusBadRequest)
		default:
			d.log.Warn("handler failed",
				zap.String("event_type", evt.EventType), zap.Error(err))
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}
	if d.afterHandle != nil {
		d.afterHandle(ctx, evt)
	}
	w.WriteHeader(http.StatusOK)
}

func validateEnvelope(evt Event) error {
	if evt.EventID == uuid.Nil || evt.EventType == "" || evt.CorrelationID == "" || evt.OccurredAt.Equal(time.Time{}) {
		return errors.New("dispatcher: event_id, event_type, correlation_id and timestamp are required")
	}
	if len(evt.Payload) == 0 || !json.Valid(evt.Payload) {
		return errors.New("dispatcher: payload must be valid JSON")
	}
	return nil
}
