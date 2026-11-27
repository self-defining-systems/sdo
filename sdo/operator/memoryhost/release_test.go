package memoryhost

import (
	"testing"
	"testing/synctest"
)

// A caller that holds a run's result must be able to start the next run at once,
// so the result is delivered only after the run's slot is released.
func TestResultArrivesOnlyAfterTheSlotIsReleased(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		done := goThenRelease(func() int { return 1 }, func() { <-release })
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("result delivered while the slot was still taken")
		default:
		}
		close(release)
		if got := <-done; got != 1 {
			t.Fatalf("result = %d", got)
		}
	})
}
