package runtime

import (
	"log/slog"
	"slices"
	"strings"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

// startupSkippedRestored names a period that did not run because the restored
// controller state predates it: the install was already running.
const startupSkippedRestored = "restored-state"

// startupPeriod holds a new install's openers until the controller has seen
// the application healthy once: one settle window (DefaultStateSettle) with no
// opener active, confirming or batched, after every opener detector completed
// an evaluation in this process. Detectors still run and record; only the
// drain of the batcher waits. The period ends for good when established or at
// its bound, after which held openers open an incident the ordinary way. It
// is persisted with the controller state, whose lifetime is the install's, so
// a restart neither starts a new period nor moves the bound; the stretch and
// the set of opener detectors that ran are per process. Guarded by
// Controller.mu.
type startupPeriod struct {
	began, bound, established, boundReached time.Time
	skipped                                 string
	stretch                                 healthyStretch
	ran                                     map[string]bool
}

func newStartupPeriod(began, bound time.Time) *startupPeriod {
	return &startupPeriod{began: began, bound: bound, stretch: newHealthyStretch(DefaultStateSettle), ran: map[string]bool{}}
}

// holding reports whether openers must still wait at now.
func (p *startupPeriod) holding(now time.Time) bool {
	return p != nil && p.skipped == "" && p.established.IsZero() && p.boundReached.IsZero() && now.Before(p.bound)
}

// boundPassed reports whether the period is still open although its bound
// passed: the next dispatch decision ends it.
func (p *startupPeriod) boundPassed(now time.Time) bool {
	return p != nil && p.skipped == "" && p.established.IsZero() && p.boundReached.IsZero() && !now.Before(p.bound)
}

func (p *startupPeriod) contract() *contracts.StartupPeriod {
	if p == nil {
		return nil
	}
	return &contracts.StartupPeriod{
		BeganAt: optionalTimestamp(p.began), BoundAt: optionalTimestamp(p.bound), EstablishedAt: optionalTimestamp(p.established),
		BoundReachedAt: optionalTimestamp(p.boundReached), SkippedReason: p.skipped,
	}
}

// restoredStartupPeriod continues the persisted period of this install. State
// without a period was written by an install already running before it.
func restoredStartupPeriod(state *contracts.StartupPeriod) *startupPeriod {
	if state == nil {
		return &startupPeriod{skipped: startupSkippedRestored}
	}
	period := newStartupPeriod(controllerTime(state.GetBeganAt()), controllerTime(state.GetBoundAt()))
	period.established, period.boundReached = controllerTime(state.GetEstablishedAt()), controllerTime(state.GetBoundReachedAt())
	period.skipped = state.GetSkippedReason()
	return period
}

// openerDetector reports whether a detector's findings can open an incident
// (Controller.opens): a Kubernetes-condition or health-probe traffic detector.
func openerDetector(id string) bool {
	return slices.Contains(memory.KubernetesConditionDetectorIDs(), id) || strings.HasPrefix(id, memory.TrafficDetectorPrefix)
}

// noteStartupEvaluation records a completed evaluation of an opener detector.
func (c *Controller) noteStartupEvaluation(detectorID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.startup.holding(c.evaluationAt) && openerDetector(detectorID) {
		c.startup.ran[detectorID] = true
	}
}

// observeStartup feeds one evaluation pass to the period's stretch. Only
// openers count: detector errors, clues and onboarding do not hold it.
func (c *Controller) observeStartup(now time.Time) {
	c.mu.Lock()
	holding := c.startup.holding(now)
	c.mu.Unlock()
	if !holding {
		return
	}
	quiet := c.quiet()
	c.mu.Lock()
	defer c.mu.Unlock()
	for id := range c.detectors {
		if openerDetector(id) && !c.setupDetectorDisabled(id) && !c.startup.ran[id] {
			quiet = false
		}
	}
	if event, _ := c.startup.stretch.observe(now, quiet); event == stretchSettled {
		c.startup.established = now.UTC()
		slog.Info("controller_startup", "event", "established", "began_at", c.startup.began, "established_at", c.startup.established)
	}
}

// startupHolds is the dispatch decision: true while the period holds. The
// first decision past the bound ends the period, and held openers then
// dispatch like any other batch.
func (c *Controller) startupHolds(now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.startup.boundPassed(now) {
		c.startup.boundReached = now.UTC()
		slog.Warn("controller_startup", "event", "bound_reached", "began_at", c.startup.began, "bound_at", c.startup.bound)
	}
	return c.startup.holding(now)
}
