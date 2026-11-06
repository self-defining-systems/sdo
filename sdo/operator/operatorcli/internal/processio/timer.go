package processio

import (
	"context"
	"math/rand/v2"
	"time"
)

// SleepUntil waits for the wall clock to reach t or for ctx to end.
func SleepUntil(ctx context.Context, t time.Time) error {
	timer := time.NewTimer(time.Until(t))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Jitter returns a random fraction in [0, 1) for poll jitter.
func Jitter() float64 { return rand.Float64() }
