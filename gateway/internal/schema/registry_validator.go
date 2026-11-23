package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v5"
	"golang.org/x/sync/singleflight"
)

type ValidationObserver interface {
	ObserveSchemaCache(result string)
	ObserveSchemaValidation(path, eventType, outcome string, elapsed time.Duration)
}

type nopValidationObserver struct{}

func (nopValidationObserver) ObserveSchemaCache(string)                                     {}
func (nopValidationObserver) ObserveSchemaValidation(string, string, string, time.Duration) {}

type RegistryValidatorConfig struct {
	GroupName    string
	CacheEnabled bool
	CacheTTL     time.Duration
	Observer     ValidationObserver
	Clock        func() time.Time
}

type cachedSchema struct {
	compiled  *jsonschema.Schema
	metadata  RegistrySchema
	expiresAt time.Time
}

type RegistryValidator struct {
	client *RegistryClient
	cfg    RegistryValidatorConfig

	mu      sync.RWMutex
	cache   map[string]cachedSchema
	refresh singleflight.Group
}

type pathValidator struct {
	validator *RegistryValidator
	path      string
}

func (v *RegistryValidator) ForPath(path string) Validator {
	return pathValidator{validator: v, path: path}
}

func (v pathValidator) Validate(ctx context.Context, eventType string, payload json.RawMessage) error {
	return v.validator.ValidatePath(ctx, v.path, eventType, payload)
}

func NewRegistryValidator(client *RegistryClient, cfg RegistryValidatorConfig) (*RegistryValidator, error) {
	if client == nil {
		return nil, fmt.Errorf("schema registry client is required")
	}
	cfg.GroupName = strings.TrimSpace(cfg.GroupName)
	if cfg.GroupName == "" {
		return nil, fmt.Errorf("schema registry group name is required")
	}
	if cfg.CacheEnabled && cfg.CacheTTL <= 0 {
		return nil, fmt.Errorf("schema cache ttl must be positive")
	}
	if cfg.Observer == nil {
		cfg.Observer = nopValidationObserver{}
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	return &RegistryValidator{client: client, cfg: cfg, cache: make(map[string]cachedSchema)}, nil
}

func (v *RegistryValidator) Subject(eventType string) string {
	return v.cfg.GroupName + "." + eventType + "-value"
}

func (v *RegistryValidator) Warm(ctx context.Context, eventTypes []string) error {
	for _, eventType := range eventTypes {
		if _, err := v.schemaFor(ctx, eventType); err != nil {
			return fmt.Errorf("warm schema %s: %w", eventType, err)
		}
	}
	return nil
}

func (v *RegistryValidator) Validate(ctx context.Context, eventType string, payload json.RawMessage) error {
	return v.ValidatePath(ctx, "unknown", eventType, payload)
}

func (v *RegistryValidator) ValidatePath(ctx context.Context, path, eventType string, payload json.RawMessage) error {
	start := v.cfg.Clock()
	schema, err := v.schemaFor(ctx, eventType)
	if err != nil {
		outcome := "error"
		switch {
		case errors.Is(err, ErrSchemaNotFound):
			outcome = "not_found"
		case errors.Is(err, ErrRegistryUnavailable):
			outcome = "registry_error"
		}
		v.cfg.Observer.ObserveSchemaValidation(path, eventType, outcome, v.cfg.Clock().Sub(start))
		return err
	}
	var document any
	if err := json.Unmarshal(payload, &document); err != nil {
		v.cfg.Observer.ObserveSchemaValidation(path, eventType, "invalid", v.cfg.Clock().Sub(start))
		return fmt.Errorf("%w: invalid JSON: %v", ErrSchemaViolation, err)
	}
	if err := schema.compiled.Validate(document); err != nil {
		v.cfg.Observer.ObserveSchemaValidation(path, eventType, "invalid", v.cfg.Clock().Sub(start))
		return fmt.Errorf("%w: %v", ErrSchemaViolation, err)
	}
	v.cfg.Observer.ObserveSchemaValidation(path, eventType, "valid", v.cfg.Clock().Sub(start))
	return nil
}

func (v *RegistryValidator) schemaFor(ctx context.Context, eventType string) (cachedSchema, error) {
	now := v.cfg.Clock()
	if cached, ok := v.fresh(eventType, now); ok {
		v.cfg.Observer.ObserveSchemaCache("hit")
		return cached, nil
	}
	v.cfg.Observer.ObserveSchemaCache("miss")
	value, err, _ := v.refresh.Do(eventType, func() (any, error) {
		// Outra chamada pode ter terminado entre a primeira consulta e a
		// entrada no singleflight.
		if cached, ok := v.fresh(eventType, v.cfg.Clock()); ok {
			return cached, nil
		}
		latest, err := v.client.Latest(ctx, v.Subject(eventType))
		if err != nil {
			return cachedSchema{}, err
		}
		compiled, err := compileRegistrySchema(eventType, latest.Schema)
		if err != nil {
			return cachedSchema{}, fmt.Errorf("%w: %v", ErrRegistryUnavailable, err)
		}
		expiresAt := v.cfg.Clock()
		if v.cfg.CacheEnabled {
			expiresAt = expiresAt.Add(v.cfg.CacheTTL)
		}
		cached := cachedSchema{compiled: compiled, metadata: latest, expiresAt: expiresAt}
		v.mu.Lock()
		v.cache[eventType] = cached
		v.mu.Unlock()
		return cached, nil
	})
	if err != nil {
		return cachedSchema{}, err
	}
	return value.(cachedSchema), nil
}

func (v *RegistryValidator) fresh(eventType string, now time.Time) (cachedSchema, bool) {
	if !v.cfg.CacheEnabled {
		return cachedSchema{}, false
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	cached, ok := v.cache[eventType]
	return cached, ok && now.Before(cached.expiresAt)
}

func compileRegistrySchema(eventType string, raw json.RawMessage) (*jsonschema.Schema, error) {
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft7
	resource := "https://event-gateway.local/schemas/" + eventType + ".json"
	if err := compiler.AddResource(resource, bytes.NewReader(raw)); err != nil {
		return nil, fmt.Errorf("add registry schema resource: %w", err)
	}
	compiled, err := compiler.Compile(resource)
	if err != nil {
		return nil, fmt.Errorf("compile registry schema: %w", err)
	}
	return compiled, nil
}

func sameJSON(left, right json.RawMessage) bool {
	var a, b any
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil {
		return false
	}
	leftCanonical, _ := json.Marshal(a)
	rightCanonical, _ := json.Marshal(b)
	return bytes.Equal(leftCanonical, rightCanonical)
}
