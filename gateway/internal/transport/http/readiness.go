package http

import (
	"context"
	"fmt"
)

type CompositeReadiness []ReadinessChecker

func (c CompositeReadiness) Ping(ctx context.Context) error {
	for i, checker := range c {
		if checker == nil {
			return fmt.Errorf("readiness checker %d is nil", i)
		}
		if err := checker.Ping(ctx); err != nil {
			return err
		}
	}
	return nil
}
