// Package waitio is the applier's clock edge: the default pause between
// readiness polls. Tests inject Applier.Wait instead.
package waitio

import (
	"context"
	"time"
)

// Wait pauses for d, or until ctx ends.
func Wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
