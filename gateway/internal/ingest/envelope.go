package ingest

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Envelope é o formato aceito por POST /events. O envelope inteiro é validado
// contra o JSON Schema do event_type; os demais campos vão para colunas
// próprias.
type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	CorrelationID string          `json:"correlation_id"`
	Timestamp     time.Time       `json:"timestamp"`
	Payload       json.RawMessage `json:"payload"`
}

var ErrMissingField = errors.New("envelope: required field missing")

func (e *Envelope) Validate() error {
	if e.EventID == "" {
		return errWithField("event_id")
	}
	if _, err := uuid.Parse(e.EventID); err != nil {
		return errWithField("event_id must be uuid")
	}
	if e.EventType == "" {
		return errWithField("event_type")
	}
	if e.CorrelationID == "" {
		return errWithField("correlation_id")
	}
	if e.Timestamp.IsZero() {
		return errWithField("timestamp")
	}
	if len(e.Payload) == 0 {
		return errWithField("payload")
	}
	return nil
}

func errWithField(field string) error {
	return errors.Join(ErrMissingField, errors.New(field))
}
