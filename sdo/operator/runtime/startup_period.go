package runtime

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
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
	// holders is what kept the last pass from counting toward the healthy
	// window, or since when it has been healthy; reported, never decided on.
	holders []string
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
		BoundReachedAt: optionalTimestamp(p.boundReached), SkippedReason: p.skipped, Holders: p.reportedHolders(),
	}
}

// reportedHolders is holders while the period is open, else none.
func (p *startupPeriod) reportedHolders() []string {
	if p.skipped != "" || !p.established.IsZero() || !p.boundReached.IsZero() {
		return nil
	}
	return slices.Clone(p.holders)
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
// (Controller.opens): a Kubernetes-condition, health-probe traffic or (out of shadow mode) request-metrics detector.
func (c *Controller) openerDetector(id string) bool {
	if id == memory.RequestMetricsDetectorID {
		return c.config.RequestMetricsOpens
	}
	return slices.Contains(memory.KubernetesConditionDetectorIDs(), id) || strings.HasPrefix(id, memory.TrafficDetectorPrefix)
}

// noteStartupEvaluation records a completed evaluation of an opener detector.
func (c *Controller) noteStartupEvaluation(detectorID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.startup.holding(c.evaluationAt) && c.openerDetector(detectorID) {
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
	var holders []string
	if !quiet {
		holders = c.startupBusyHolders()
	}
	if c.config.StartupWarmupSettled != nil && !c.config.StartupWarmupSettled() {
		quiet = false
		holders = append(holders, "measurement warm-up not settled")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var unevaluated []string
	for id := range c.detectors {
		if c.openerDetector(id) && !c.startup.ran[id] {
			quiet = false
			unevaluated = append(unevaluated, id)
		}
	}
	if len(unevaluated) > 0 {
		slices.Sort(unevaluated)
		holders = append(holders, "not yet evaluated: "+boundedLabels(unevaluated))
	}
	event, start := c.startup.stretch.observe(now, quiet)
	switch {
	case event == stretchSettled:
		c.startup.established = now.UTC()
		holders = nil
		slog.Info("controller_startup", "event", "established", "began_at", c.startup.began, "established_at", c.startup.established)
	case quiet:
		holders = []string{fmt.Sprintf("healthy since %s; established after %s", start.UTC().Format(time.RFC3339), c.startup.stretch.settle)}
	}
	c.startup.holders = holders
}

// startupHoldersShown is how many findings one holder line names before it
// counts the rest.
const startupHoldersShown = 8

// startupBusyHolders names why the controller is not quiet (Controller.quiet):
// an open incident or pending closure, the firing or confirming openers, and
// the batched findings not already named. Takes c.mu itself.
func (c *Controller) startupBusyHolders() []string {
	var holders []string
	c.mu.Lock()
	if c.incidentOpen {
		holders = append(holders, "incident open")
	}
	if c.closureBlocksNextLocked() {
		holders = append(holders, "closure pending")
	}
	c.mu.Unlock()
	var firing []sdk.Finding
	named := map[string]bool{}
	for _, state := range c.tracker.Snapshot() {
		if (state.Active || state.FiringCount > 0) && c.opens(state.Finding) {
			finding := state.Finding
			if finding.DetectorID == "" {
				finding.DetectorID = state.DetectorID
			}
			firing = append(firing, finding)
			named[detectFindingLabel(finding)] = true
		}
	}
	var batched []sdk.Finding
	for _, finding := range c.batcher.Snapshot().Findings {
		if !named[detectFindingLabel(finding)] {
			batched = append(batched, finding)
		}
	}
	if len(firing) > 0 {
		holders = append(holders, "firing: "+startupHolderLabels(firing))
	}
	if len(batched) > 0 {
		holders = append(holders, "batched: "+startupHolderLabels(batched))
	}
	return holders
}

// startupHolderLabels names findings the way the agent addresses them,
// de-duplicated, critical first, bounded.
func startupHolderLabels(findings []sdk.Finding) string {
	rank := map[sdk.FindingSeverity]int{sdk.SeverityCritical: 0, sdk.SeverityWarning: 1, sdk.SeverityInfo: 2}
	seen := map[string]int{}
	for _, finding := range findings {
		label, severity := detectFindingLabel(finding), len(rank)
		if r, ok := rank[finding.Severity]; ok {
			severity = r
		}
		if previous, ok := seen[label]; !ok || severity < previous {
			seen[label] = severity
		}
	}
	labels := make([]string, 0, len(seen))
	for label := range seen {
		labels = append(labels, label)
	}
	slices.SortFunc(labels, func(a, b string) int {
		if seen[a] != seen[b] {
			return seen[a] - seen[b]
		}
		return strings.Compare(a, b)
	})
	return boundedLabels(labels)
}

// boundedLabels joins the first startupHoldersShown labels and counts the rest.
func boundedLabels(labels []string) string {
	if len(labels) <= startupHoldersShown {
		return strings.Join(labels, ", ")
	}
	return fmt.Sprintf("%s (+%d more)", strings.Join(labels[:startupHoldersShown], ", "), len(labels)-startupHoldersShown)
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
