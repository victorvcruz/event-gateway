// Package logging cria o *zap.Logger padrão de todos os serviços, com helpers
// With* para os campos service_id, event_id e correlation_id.
package logging

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Config struct {
	ServiceID string
	Level     string
	Encoding  string
}

const (
	FieldServiceID     = "service_id"
	FieldEventID       = "event_id"
	FieldCorrelationID = "correlation_id"
)

func New(cfg Config) (*zap.Logger, error) {
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}

	encoding := cfg.Encoding
	if encoding == "" {
		encoding = "json"
	}

	zapCfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(level),
		Encoding:         encoding,
		EncoderConfig:    zap.NewProductionEncoderConfig(),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	zapCfg.EncoderConfig.TimeKey = "ts"
	zapCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	base, err := zapCfg.Build()
	if err != nil {
		return nil, fmt.Errorf("build zap logger: %w", err)
	}

	if cfg.ServiceID != "" {
		base = base.With(zap.String(FieldServiceID, cfg.ServiceID))
	}
	return base, nil
}

func parseLevel(raw string) (zapcore.Level, error) {
	if raw == "" {
		return zapcore.InfoLevel, nil
	}
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(raw)); err != nil {
		return zapcore.InfoLevel, fmt.Errorf("invalid log level %q: %w", raw, err)
	}
	return lvl, nil
}

type ctxKey int

const (
	ctxKeyEventID ctxKey = iota
	ctxKeyCorrelationID
)

func WithEventID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyEventID, id)
}

func EventID(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(ctxKeyEventID).(string)
	return v, ok && v != ""
}

func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyCorrelationID, id)
}

func CorrelationID(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(ctxKeyCorrelationID).(string)
	return v, ok && v != ""
}

func FromContext(ctx context.Context, base *zap.Logger) *zap.Logger {
	fields := make([]zap.Field, 0, 2)
	if id, ok := EventID(ctx); ok {
		fields = append(fields, zap.String(FieldEventID, id))
	}
	if id, ok := CorrelationID(ctx); ok {
		fields = append(fields, zap.String(FieldCorrelationID, id))
	}
	if len(fields) == 0 {
		return base
	}
	return base.With(fields...)
}
