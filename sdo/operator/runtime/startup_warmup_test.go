package runtime

import (
	"context"
	"testing"
	"time"

	clocktesting "k8s.io/utils/clock/testing"
)

// BeginWarmup returns at once; the observer reports unsettled until the wait
// concludes, then calls done, and Stop abandons a wait without calling it.
func TestBeginWarmupRunsInTheBackground(t *testing.T) {
	workload := pacedWorkload(1, 5)
	concluded := make(chan bool, 1)
	clock := clocktesting.NewFakeClock(time.Unix(1000, 0).UTC())
	observer := NewTrafficObserver(&stalledProber{pacedProber: pacedProber{clock: clock, started: clock.Now(), workload: workload, interval: time.Second}}, []string{workload.Name}, nil, 0)
	observer.Clock = clock
	if !observer.WarmupSettled() {
		t.Fatal("an observer never asked to warm up reports settled")
	}
	observer.BeginWarmup(context.Background(), 200*time.Millisecond, func(warm bool) { concluded <- warm })
	if observer.WarmupSettled() {
		t.Fatal("settled before the wait concluded")
	}
	if warm := <-concluded; warm {
		t.Fatal("a prober with no sample reported warm")
	}
	if !observer.WarmupSettled() {
		t.Fatal("not settled after the wait concluded")
	}
	observer.BeginWarmup(context.Background(), time.Hour, func(bool) { t.Error("done called for an abandoned wait") })
	observer.Stop()
	if observer.WarmupSettled() {
		t.Fatal("an abandoned wait must not count as settled")
	}
}

// The start-up period still waits for the traffic warm-up to conclude before a
// settle window counts as healthy, though the controller serves meanwhile.
func TestStartupPeriodWaitsForTheTrafficWarmup(t *testing.T) {
	probe := probeSamples(1, func(int) bool { return false })
	c := startupController(t, 30*time.Minute, 0, probe)
	settled := false
	c.config.StartupWarmupSettled = func() bool { return settled }
	for second := int64(0); second <= 200; second++ {
		step(t, c, second)
	}
	if startupState(t, c).GetEstablishedAt() != nil {
		t.Fatal("established while the traffic warm-up was still running")
	}
	settled = true
	for second := int64(201); second <= 400; second++ {
		step(t, c, second)
	}
	if startupState(t, c).GetEstablishedAt() == nil {
		t.Fatal("not established after the warm-up concluded and a settle window passed")
	}
}
