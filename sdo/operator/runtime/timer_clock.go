package runtime

import (
	"context"
	kclock "k8s.io/utils/clock"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"time"
)

func timerClock(clock kclock.WithTicker) kclock.WithTicker {
	if clock != nil {
		return clock
	}
	return lifecycleio.Clock()
}

func waitOnClock(ctx context.Context, clock kclock.Clock, delay time.Duration) error {
	timer := clock.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
