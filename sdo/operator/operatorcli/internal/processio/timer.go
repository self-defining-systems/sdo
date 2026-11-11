package processio

import (
	"context"
	"math/rand/v2"
	"os/signal"
	"syscall"
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

// TerminationContext ends when the process receives SIGTERM or SIGINT, so a sidecar can finish its last pass.
func TerminationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
}
