// Package delivery cuida da saída dos eventos do gateway: o sink de
// dead-letter, o sender HTTP e o hook de sucesso do dispatch. Aqui também fica
// o wrapper DispatchEnvelope, que leva service_id e callback_url de
// dispatch_outbox até o Sender sem alterar shared/outbox.Row.
//
// O wrapper nunca chega ao subscriber: o body de POST /dispatch é sempre o
// DispatchEnvelope.Envelope cru.
package delivery

import (
	"encoding/json"
	"errors"
	"fmt"
)

type TargetMeta struct {
	ServiceID   string `json:"service_id"`
	CallbackURL string `json:"callback_url"`
}

type DispatchEnvelope struct {
	Target   TargetMeta      `json:"target"`
	Envelope json.RawMessage `json:"envelope"`
}

var ErrInvalidWrapper = errors.New("delivery: invalid dispatch wrapper")

// EncodeDispatchEnvelope monta o wrapper JSON gravado em dispatch_outbox.payload.
// envelope são os bytes originais do evento (o body de POST /events), mantidos
// byte a byte: o gateway não interpreta o payload de negócio.
func EncodeDispatchEnvelope(target TargetMeta, envelope json.RawMessage) ([]byte, error) {
	if target.ServiceID == "" {
		return nil, fmt.Errorf("%w: service_id is required", ErrInvalidWrapper)
	}
	if target.CallbackURL == "" {
		return nil, fmt.Errorf("%w: callback_url is required", ErrInvalidWrapper)
	}
	if len(envelope) == 0 {
		return nil, fmt.Errorf("%w: envelope is empty", ErrInvalidWrapper)
	}
	return json.Marshal(DispatchEnvelope{Target: target, Envelope: envelope})
}

// DecodeDispatchEnvelope decodifica o wrapper de dispatch_outbox.payload.
// Falha se o JSON estiver corrompido ou o target vazio. Quem chama (HTTPSender,
// SuccessHook, DeadLetterSink) trata como não retentável, já que tentar de novo
// não conserta o payload.
func DecodeDispatchEnvelope(payload []byte) (DispatchEnvelope, error) {
	var w DispatchEnvelope
	if err := json.Unmarshal(payload, &w); err != nil {
		return w, fmt.Errorf("%w: %v", ErrInvalidWrapper, err)
	}
	if w.Target.ServiceID == "" {
		return w, fmt.Errorf("%w: target.service_id is empty", ErrInvalidWrapper)
	}
	if w.Target.CallbackURL == "" {
		return w, fmt.Errorf("%w: target.callback_url is empty", ErrInvalidWrapper)
	}
	if len(w.Envelope) == 0 {
		return w, fmt.Errorf("%w: envelope is empty", ErrInvalidWrapper)
	}
	return w, nil
}
