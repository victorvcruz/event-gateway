package dispatcher

import (
	"context"

	"go.uber.org/zap"
)

type options struct {
	log         *zap.Logger
	afterHandle func(context.Context, Event)
}

// WithAfterHandle registra um hook chamado depois que o handler tem sucesso e
// antes da resposta HTTP. O hook deve ser rápido.
func WithAfterHandle(hook func(context.Context, Event)) Option {
	return func(o *options) { o.afterHandle = hook }
}

type Option func(*options)

// WithLogger define o *zap.Logger. O padrão é zap.NewNop().
func WithLogger(l *zap.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.log = l
		}
	}
}
