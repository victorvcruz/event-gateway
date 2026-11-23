package schema

import (
	"context"
	"encoding/json"
	"errors"
)

//go:generate mockgen -source=$GOFILE -destination=./mocks/mock_schema.go -package=mocks

type Validator interface {
	Validate(ctx context.Context, eventType string, payload json.RawMessage) error
}

var ErrSchemaNotFound = errors.New("schema not found")

var (
	ErrSchemaViolation     = errors.New("schema violation")
	ErrRegistryUnavailable = errors.New("schema registry unavailable")
	ErrIncompatibleSchema  = errors.New("incompatible schema")
)
