package processio

import (
	"context"
	"math/rand/v2"
	"os"
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

// Termination is TerminationContext plus an escalation: the first SIGTERM or
// SIGINT ends the context (a graceful stop), a second one closes force (stop
// now). stop releases the signal handling.
func Termination(ctx context.Context) (context.Context, <-chan struct{}, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	force := make(chan struct{})
	done := make(chan struct{})
	go func() {
		for first := true; ; first = false {
			select {
			case <-signals:
				if first {
					cancel()
					continue
				}
				close(force)
				return
			case <-done:
				return
			}
		}
	}()
	return ctx, force, func() {
		signal.Stop(signals)
		close(done)
		cancel()
	}
}
