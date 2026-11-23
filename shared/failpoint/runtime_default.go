//go:build !chaos

package failpoint

import (
	"context"

	"go.uber.org/zap"
)

type noopProbe struct{}

func newRuntime(_ string, _ *zap.Logger) Probe { return noopProbe{} }

func (noopProbe) Hit(context.Context, Checkpoint) {}
