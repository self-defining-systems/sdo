package traffic

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"
)

const (
	// confirmDelay is how long after a health-probe scenario starts failing it
	// is sampled again, so a failure is confirmed by evidence and not by
	// waiting for healthy samples to leave the SLO window.
	confirmDelay = time.Second
	// confirmStallSpan is how far apart the first and the newest failing
	// sample must start: a restart answers 5xx, and a worker node's data plane
	// stalls after some pod-network changes, for about 3s.
	confirmStallSpan = 4 * time.Second
	// confirmPeerWindow is how close in time the failures of two scenarios
	// must end to confirm each other.
	confirmPeerWindow = 15 * time.Second
)

// failingRun is a scenario's current failure: its newest usable samples, all
// failing, since a success.
type failingRun struct {
	count int
	slow  time.Duration
	// first and last are when the oldest and the newest of them began, and end
	// is when the last ended.
	first, last, end time.Time
}

func (s Sample) end() time.Time { return s.At.Add(s.Latency) }

// failed reports an error, timeout or completed run over the latency budget.
func (s Sample) failed(slo SLO) bool {
	return s.Outcome == OutcomeError || s.Outcome == OutcomeTimeout || s.slow(slo)
}

func (s Sample) slow(slo SLO) bool {
	return s.Outcome == OutcomeOK && slo.MaxLatency > 0 && s.Latency > slo.MaxLatency.Duration()
}

// currentRun is the scenario's failing run at now; its count is zero when the
// newest usable sample succeeded, or when no success precedes the run. Samples
// are ordered by their start: a request that hangs does not hide a later
// success, and one that began before the failure does not refute it.
func currentRun(observed ScenarioObservations, slo SLO, now time.Time) failingRun {
	ordered := make([]Sample, 0, len(observed.Samples)+len(observed.Confirming))
	for _, sample := range slices.Concat(observed.Samples, observed.Confirming) {
		if sample.Outcome != OutcomeInvalid {
			ordered = append(ordered, sample)
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].At.Before(ordered[j].At) })
	var run failingRun
	for i := len(ordered) - 1; i >= 0; i-- {
		sample := ordered[i]
		if !sample.failed(slo) {
			return run
		}
		if now.Sub(sample.At) > slo.MaxAge.Duration() {
			continue
		}
		if run.count == 0 {
			run.last = sample.At
		}
		run.count++
		if sample.slow(slo) {
			run.slow = max(run.slow, sample.Latency)
		}
		run.first = sample.At
		if end := sample.end(); end.After(run.end) {
			run.end = end
		}
	}
	return failingRun{}
}

// StartsFailing reports whether a qualified scenario's newest usable sample
// failed right after a success, so it is worth sampling again now.
func StartsFailing(observed ScenarioObservations, slo SLO, now time.Time) bool {
	return observed.Qualified && currentRun(observed, slo, now).count == 1
}

// ConfirmedFailures are the scenarios whose failure needs no further samples,
// as "workload/scenario" to the reason: failing samples with no success between
// them, or of two user tests ending within confirmPeerWindow, that began at
// least confirmStallSpan apart. Only qualified, evaluated health-probe
// scenarios that are not KnownBroken count. One failing sample never confirms
// itself, and a partial failure is left to the SLO.
func ConfirmedFailures(windows map[string]Window) map[string]string {
	type candidate struct {
		key, scenario string
		userTest      bool
		run           failingRun
		budget        time.Duration
	}
	var failing []candidate
	for name, window := range windows {
		if window.Workload.Purpose != PurposeHealthProbe || window.LoadError != "" {
			continue
		}
		for _, observed := range window.Scenarios {
			id := observed.Scenario.ID
			if !observed.Qualified || window.Workload.KnownBrokenScenario(id) != nil || !scenarioHasEnoughSamples(window, observed) {
				continue
			}
			slo := window.Workload.ScenarioSLO(id)
			if run := currentRun(observed, slo, window.ObservedAt); run.count > 0 {
				failing = append(failing, candidate{name + "/" + id, id, observed.UserTest, run, slo.MaxLatency.Duration()})
			}
		}
	}
	sort.Slice(failing, func(i, j int) bool { return failing[i].key < failing[j].key })
	confirmed := map[string]string{}
	for i, one := range failing {
		run := one.run
		if span := run.last.Sub(run.first); span >= confirmStallSpan {
			confirmed[one.key] = fmt.Sprintf("%d failing samples in a row over %s", run.count, span.Round(time.Millisecond))
		}
		for _, peer := range failing[i+1:] {
			if !one.userTest || !peer.userTest || peer.scenario == one.scenario || run.end.Sub(peer.run.end).Abs() > confirmPeerWindow {
				continue
			}
			first, last := run.first, run.last
			if peer.run.first.Before(first) {
				first = peer.run.first
			}
			if peer.run.last.After(last) {
				last = peer.run.last
			}
			if last.Sub(first) < confirmStallSpan {
				continue
			}
			for _, pair := range [][2]candidate{{one, peer}, {peer, one}} {
				if confirmed[pair[0].key] == "" {
					confirmed[pair[0].key] = "failing at the same time as " + pair[1].key
				}
			}
		}
	}
	for _, one := range failing {
		if reason := confirmed[one.key]; reason != "" && one.run.slow > 0 {
			confirmed[one.key] = fmt.Sprintf("%s (iteration latency %s > %s)", reason, one.run.slow.Round(time.Millisecond), one.budget)
		}
	}
	return confirmed
}

// UseConfirm makes Run sample a scenario again when startsFailing says its
// scheduled sample began a failure (StartsFailing), and hands those samples to
// record instead of Run's emit.
func (e *Engine) UseConfirm(startsFailing func(scenario string) bool, record func(scenario string, sample Sample)) {
	e.startsFailing, e.confirmed = startsFailing, record
}

// confirmer runs the confirming samples of one Run: at most two per failure,
// one failure per scenario at a time, inside the run's in-flight slots.
type confirmer struct {
	engine *Engine
	slots  chan struct{}
	emit   func(scenario string, sample Sample)
	mu     sync.Mutex
	active map[string]bool
	next   uint64
}

// start reserves the scenario's confirmation, unless one is running.
func (c *confirmer) start(scenario string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active[scenario] {
		return false
	}
	c.active[scenario] = true
	return true
}

// run samples scenario again confirmDelay after its first failing sample
// ended, and once more confirmStallSpan after it began unless the failure spans that
// already. It stops at the first sample that is not a failure.
func (c *confirmer) run(ctx context.Context, scenario string, first Sample, slo SLO) {
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		delete(c.active, scenario)
	}()
	for _, due := range []time.Time{c.engine.clock.Now().Add(confirmDelay), first.At.Add(confirmStallSpan)} {
		select {
		case <-ctx.Done():
			return
		case <-c.engine.clock.After(due.Sub(c.engine.clock.Now())):
		}
		select {
		case <-ctx.Done():
			return
		case c.slots <- struct{}{}:
		}
		c.mu.Lock()
		iteration, _ := c.engine.IterationOf(scenario, c.next)
		c.next = iteration + 1
		c.mu.Unlock()
		_, sample := c.engine.Iterate(ctx, iteration)
		<-c.slots
		if ctx.Err() != nil && sample.Outcome != OutcomeOK {
			return
		}
		c.emit(scenario, sample)
		if !sample.failed(slo) || sample.At.Sub(first.At) >= confirmStallSpan {
			return
		}
	}
}
