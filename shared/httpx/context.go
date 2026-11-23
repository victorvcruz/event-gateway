package httpx

import "context"

type ctxKey int

const (
	ctxKeyCorrelationID ctxKey = iota
	ctxKeyIdempotencyKey
)

func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyCorrelationID, id)
}

func CorrelationID(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(ctxKeyCorrelationID).(string)
	return v, ok && v != ""
}

func WithIdempotencyKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, ctxKeyIdempotencyKey, key)
}

func IdempotencyKey(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(ctxKeyIdempotencyKey).(string)
	return v, ok && v != ""
}
