// Package waitio is the lifecycle's clock edge: the default pause between
// bounded re-evaluations. Tests inject Coordinator.Wait instead.
package waitio

import (
	"context"
	"time"
)

// Now is the wall clock.
func Now() time.Time { return time.Now() }

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
