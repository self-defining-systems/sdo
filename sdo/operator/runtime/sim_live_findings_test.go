package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

// Maintenance is scheduled on the same virtual clock as detector observations.
// In particular, no detector Step is allowed to rescue a paused early report.
func TestSimSchedulePauseAfterEarlyClosure(t *testing.T) {
	sweepSimSeedsN(t, 8, func(t *testing.T, seed uint64) {
		rng := newSimRNG(seed)
		for _, restart := range []bool{false, true} {
			t.Run(fmt.Sprintf("restart=%t", restart), func(t *testing.T) {
				ctx := context.Background()
				c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
				c.broker = &recordingIncidentBroker{}
				for i := 2; i <= 17; i++ {
					step(i)
				}
				closure, ok := c.PendingIncidentClosure()
				if !ok || !closure.VerifiedWhileResponderRunning {
					t.Fatal("fixture did not close with a running responder")
				}
				now := closure.VerifiedAt.Add(time.Duration(1+rng.IntN(29)) * time.Second)
				c.now = func() time.Time { return now }
				if restart {
					state := exportState(t, c)
					restored, err := NewController(c.config, c.detectorList(), c.provider, c.dispatcher, now)
					if err != nil {
						t.Fatal(err)
					}
					restored.broker = c.broker
					restored.now = c.now
					if err := restored.RestoreState(state); err != nil {
						t.Fatal(err)
					}
					c = restored
				}
				deadline := closure.VerifiedAt.Add(c.config.ResponderReportGrace)
				if wake := c.PausedWake(); !wake.Equal(deadline) {
					t.Errorf("paused wake=%v, want report deadline %v", wake, deadline)
				}
				if err := executePausedEffects(ctx, c); err != nil {
					t.Fatal(err)
				}
				if _, ok := c.PendingClosureEffect(); ok {
					t.Fatal("paused maintenance skipped report grace")
				}
				now = deadline
				if err := executePausedEffects(ctx, c); err != nil {
					t.Fatal(err)
				}
				if c.currentIncidentRequest != nil {
					t.Fatal("maintenance stranded the pending early report past its grace")
				}
				got, ok := c.PendingIncidentClosure()
				if !ok || !got.VerifiedAt.Equal(closure.VerifiedAt) {
					t.Fatal("maintenance changed the verified closure")
				}
			})
		}
	})
}
