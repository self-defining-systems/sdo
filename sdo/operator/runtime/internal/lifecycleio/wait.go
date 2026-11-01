package lifecycleio

import (
	"context"
	"math/rand/v2"
	"time"
)

// Jitter supplies the default bounded retry random draw.
func Jitter(bound int64) int64 { return rand.Int64N(bound) }

// Wait bounds a default executor delay with cancellation. Simulation callers
// inject their wait into the executor instead.
func Wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DispatchJitter supplies the default retry jitter fraction.
func DispatchJitter() float64 { return rand.Float64() }
