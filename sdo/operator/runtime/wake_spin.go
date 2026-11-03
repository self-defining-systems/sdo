package runtime

import (
	"fmt"
	"time"
)

// Production passes persist state to the API server, so a thousand zero-delay
// passes inside a second is never work: it is a wake time that no pass
// consumes.
const (
	wakeSpinPasses = 1000
	wakeSpinWindow = time.Second
)

// ErrWakeSpin is returned by the run loop when it keeps running passes with no
// delay because NextWake reports an item as due that the pass does not consume.
type ErrWakeSpin struct {
	Passes int
	Window time.Duration
	Wake   time.Time
	Reason string
}

func (e ErrWakeSpin) Error() string {
	return fmt.Sprintf(
		"controller loop ran %d passes with no delay within %s; NextWake keeps reporting %q due at %s and no pass consumes it",
		e.Passes, e.Window, e.Reason, e.Wake.Format(time.RFC3339Nano),
	)
}

// wakeSpinGuard counts consecutive timer passes that were due immediately.
type wakeSpinGuard struct {
	passes int
	since  time.Time
}

// idle ends the current run of immediate passes.
func (g *wakeSpinGuard) idle() { g.passes = 0 }

// pass records a timer pass that started after delay. It returns an error once
// the immediate passes of one window reach the limit.
func (g *wakeSpinGuard) pass(now time.Time, delay time.Duration, wake time.Time, reason string) error {
	if delay > 0 {
		g.passes = 0
		return nil
	}
	if g.passes == 0 || now.Sub(g.since) > wakeSpinWindow {
		g.passes, g.since = 0, now
	}
	g.passes++
	if g.passes < wakeSpinPasses {
		return nil
	}
	return ErrWakeSpin{Passes: g.passes, Window: now.Sub(g.since), Wake: wake, Reason: reason}
}
