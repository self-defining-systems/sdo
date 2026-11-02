package runtime

import (
	"context"
	"sync/atomic"
	"time"

)

// The setup caller's cancellation boundary is driven by virtual elapsed time.
// It uses no wall timer and never waits 360 real seconds for a slow worker.
type simSetupBudget struct {
	context.Context
	clock   *simClock
	expires time.Time
	done    chan struct{}
	expired atomic.Bool
}

func (b *simSetupBudget) Done() <-chan struct{} { return b.done }

func (b *simSetupBudget) Err() error {
	if b.expired.Load() {
		return context.DeadlineExceeded
	}
	return nil
}

func (b *simSetupBudget) spend(d time.Duration) {
	b.clock.Advance(d)
	if !b.clock.Now().Before(b.expires) {
		b.clock.Set(b.expires)
		if b.expired.CompareAndSwap(false, true) {
			close(b.done)
		}
	}
}

