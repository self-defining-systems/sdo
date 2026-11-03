package sim

import (
	"fmt"
	"sort"
	"time"
)

// The two behavioral properties of the controller we check now, each a plain
// function over the Log: the controller is reliable (EveryIncidentEnds) and it
// manages detectors well (DetectionKeepsRunning). Agents, agent-written programs and the cluster are untrusted inputs:
// every property must hold whatever they do, so the checks never consult what
// an agent claimed, only what the controller did and observed.
//
// A check returns nil, or a *Violation (as an error) naming the property, the
// incident or finding concerned, the offending log index and what was expected.

// Violation is a failed property.
type Violation struct {
	Property string
	// Entry is the index in the log of the offending entry, -1 when the
	// violation is an absence.
	Entry  int
	Detail string
}

func (v *Violation) Error() string {
	if v.Entry >= 0 {
		return fmt.Sprintf("property %q violated at log[%d]: %s", v.Property, v.Entry, v.Detail)
	}
	return fmt.Sprintf("property %q violated: %s", v.Property, v.Detail)
}

func violate(property string, entry int, format string, args ...any) error {
	return &Violation{Property: property, Entry: entry, Detail: fmt.Sprintf(format, args...)}
}

// Property names, as reported in Violation.Property.
const (
	PropEveryIncidentEnds = "every-incident-ends"
	PropDetectionRuns     = "detection-keeps-running"
)

// Config holds the run-specific bounds the checks need.
type Config struct {
	// GapFactor bounds the time between two evaluations of a detector at
	// GapFactor times its own configured interval ("a few times its interval").
	GapFactor int
	// Slack is added to every detection bound, for the controller's own tick
	// and the virtual time one evaluation takes.
	Slack time.Duration
	// Hung names detectors the run made hang in the host. They are exempt from
	// their own bound (a hang is the detector's own fault); every other
	// detector must keep its bound while they hang.
	Hung map[string]bool
	// Settle is the least time a run must last after its faults stop for every
	// incident to be expected to end.
	Settle time.Duration
	// Restart is how long after the faults stop the controller may still be
	// restarting (a lease to expire, informers to sync). Later it must stay up.
	Restart time.Duration
}

func (c Config) bound(interval time.Duration) time.Duration {
	return time.Duration(c.GapFactor)*interval + c.Slack
}

// Check runs both checks and returns every violation.
func Check(log Log, cfg Config) []error {
	var out []error
	for _, err := range []error{EveryIncidentEnds(log, cfg), DetectionKeepsRunning(log, cfg)} {
		if err != nil {
			out = append(out, err)
		}
	}
	return out
}

// EveryIncidentEnds is the reliability check.
//
// Each Opened incident has a terminal Outcome (Closed or Failed) by the
// RunEnded entry, and its outcomes never disagree. The run must be one with
// bounded faults: the log contains a FaultsQuiesced entry (no fault is injected
// at or after it), at most one, and RunEnded is at least cfg.Settle after it.
// Without that the check reports the run itself as malformed instead of passing
// vacuously. A run's faults are bounded when they stop; a controller may take
// any number of failures and restarts before that.
//
// A failed incident never blocks a later one: when an incident is still without
// an outcome at RunEnded and a Failed incident's outcome is not later than it
// (so the failed one finished while the later one was open or opened after),
// the violation names the blocking incident. Termination of every later
// incident is the whole content of "not blocked", so this is the same check
// with a sharper message.
func EveryIncidentEnds(log Log, cfg Config) error {
	var quiesced, ended *time.Time
	for i, entry := range log {
		switch e := entry.(type) {
		case FaultsQuiesced:
			if quiesced != nil {
				return violate(PropEveryIncidentEnds, i, "faults quiesced twice; the fault bound must be one instant")
			}
			at := e.At
			quiesced = &at
		case RunEnded:
			at := e.At
			ended = &at
		}
	}
	if quiesced == nil || ended == nil {
		return violate(PropEveryIncidentEnds, -1, "run has no FaultsQuiesced or RunEnded entry, so its faults are not shown to be bounded")
	}
	if ended.Sub(*quiesced) < cfg.Settle {
		return violate(PropEveryIncidentEnds, -1, "run ends %s after its faults stop, less than the required settle time %s", ended.Sub(*quiesced), cfg.Settle)
	}
	// Faults are transient, so the controller process never ends on its own: a
	// kill by the harness is a ControllerDown, an exit is the controller giving
	// up on an error it should have retried (an API 500, a lost connection).
	for i, entry := range log {
		if e, ok := entry.(ControllerExited); ok {
			return violate(PropEveryIncidentEnds, i, "the controller exited on its own at %s: %s (a transient fault must be retried, never end the process)", e.At.Format(time.RFC3339Nano), e.Reason)
		}
	}
	// A controller that keeps dying after the faults stopped is wedged, however
	// quickly it is relaunched: the restart cycle makes it look "down" to the
	// detection check, which excuses downtime.
	for i, entry := range log {
		if e, ok := entry.(ControllerDown); ok && e.At.After(quiesced.Add(cfg.Restart)) {
			return violate(PropEveryIncidentEnds, i, "the controller went down at %s, %s after its faults stopped (it may restart for %s): it keeps failing without faults", e.At.Format(time.RFC3339Nano), e.At.Sub(*quiesced), cfg.Restart)
		}
	}
	opened := map[string]int{}
	var order []string
	outcome := map[string]Outcome{}
	for i, entry := range log {
		switch e := entry.(type) {
		case Opened:
			if _, dup := opened[e.Incident]; !dup {
				opened[e.Incident] = i
				order = append(order, e.Incident)
			}
		case Outcome:
			if prior, ok := outcome[e.Incident]; ok && prior.Result != e.Result {
				return violate(PropEveryIncidentEnds, i, "incident %q has outcomes %q and %q", e.Incident, prior.Result, e.Result)
			}
			if _, ok := outcome[e.Incident]; !ok {
				outcome[e.Incident] = e
			}
		}
	}
	for _, id := range order {
		out, ok := outcome[id]
		if ok && !out.At.After(*ended) {
			continue
		}
		detail := fmt.Sprintf("incident %q opened at %s has no terminal outcome by the end of the run at %s", id, log[opened[id]].Time().Format(time.RFC3339Nano), ended.Format(time.RFC3339Nano))
		for _, other := range order {
			if o := outcome[other]; o.Result == Failed && opened[other] < opened[id] {
				detail += fmt.Sprintf("; it is blocked behind failed incident %q (failed at %s)", other, o.At.Format(time.RFC3339Nano))
				break
			}
		}
		return violate(PropEveryIncidentEnds, opened[id], "%s", detail)
	}
	return nil
}

// DetectionKeepsRunning is the detector-management check.
//
// Every detector in the active memory revision is evaluated within a few times
// its own configured interval (cfg.GapFactor times the interval, plus
// cfg.Slack). For each detector d and each period during which d is active, the
// time between consecutive evaluations of d, between the start of the period
// and its first evaluation, and between its last evaluation and the end of the
// period, is at most d's bound. A failed attempt (an error or a panic) counts
// as an evaluation: a broken detector must not delay the others, and the
// controller still has to schedule it. A detector in cfg.Hung never returns and
// is exempt from its own bound only.
//
// d is active from an Activated entry that lists it until the next Activated
// entry that does not (or RunEnded), so a revision swap, incident closure and
// agent work all count against the bound. A newly listed detector must start
// within its bound (no controller restart is assumed: ControllerDown entries are
// the only excuse); a detector an Activated entry removes must not be evaluated
// more than one bound after the swap (an evaluation already in flight may land).
// Time while the controller or host is down (ControllerDown to the next
// ControllerUp, or the end of the run) is not counted: nothing runs then, and
// the restart's own recovery time is bounded by the same gap once it is up.
// The first Activated entry must precede every Evaluated entry.
func DetectionKeepsRunning(log Log, cfg Config) error {
	if cfg.GapFactor <= 0 {
		return violate(PropDetectionRuns, -1, "detection gap factor is not configured")
	}
	var end time.Time
	var downs [][2]time.Time // [down, up]; zero up means still down
	for _, entry := range log {
		switch e := entry.(type) {
		case RunEnded:
			end = e.At
		case ControllerDown:
			downs = append(downs, [2]time.Time{e.At, {}})
		case ControllerUp:
			if n := len(downs); n > 0 && downs[n-1][1].IsZero() {
				downs[n-1][1] = e.At
			}
		}
	}
	if end.IsZero() {
		return violate(PropDetectionRuns, -1, "run has no RunEnded entry")
	}
	for i := range downs {
		if downs[i][1].IsZero() || downs[i][1].After(end) {
			downs[i][1] = end
		}
	}
	// activeSince[d] is the start of d's current period; evaluations[d] its times.
	activeSince := map[string]time.Time{}
	interval := map[string]time.Duration{}
	evaluations := map[string][]time.Time{}
	retiredAt := map[string]time.Time{}
	retiredBound := map[string]time.Duration{}
	closePeriod := func(d string, until time.Time, at int) error {
		defer func() {
			delete(activeSince, d)
			delete(evaluations, d)
		}()
		if cfg.Hung[d] {
			return nil
		}
		limit := cfg.bound(interval[d])
		points := append([]time.Time{activeSince[d]}, evaluations[d]...)
		points = append(points, until)
		for k := 1; k < len(points); k++ {
			if gap := points[k].Sub(points[k-1]) - downtimeBetween(downs, points[k-1], points[k]); gap > limit {
				return violate(PropDetectionRuns, at, "detector %q was not evaluated for %s (limit %s = %d x its %s interval) between %s and %s", d, gap, limit, cfg.GapFactor, interval[d], points[k-1].Format(time.RFC3339Nano), points[k].Format(time.RFC3339Nano))
			}
		}
		return nil
	}
	activated := false
	for i, entry := range log {
		switch e := entry.(type) {
		case Activated:
			activated = true
			listed := map[string]DetectorDecl{}
			for _, d := range e.Detectors {
				listed[d.ID] = d
			}
			var drop []string
			for d := range activeSince {
				if _, ok := listed[d]; !ok {
					drop = append(drop, d)
				}
			}
			sort.Strings(drop)
			for _, d := range drop {
				retiredAt[d], retiredBound[d] = e.At, cfg.bound(interval[d])
				if err := closePeriod(d, e.At, i); err != nil {
					return err
				}
			}
			for _, d := range e.Detectors {
				if _, on := activeSince[d.ID]; !on {
					activeSince[d.ID] = e.At
					delete(retiredAt, d.ID)
				}
				interval[d.ID] = d.Interval
			}
		case Evaluated:
			if !activated {
				return violate(PropDetectionRuns, i, "detector %q evaluated before any memory revision was activated", e.Detector)
			}
			if _, on := activeSince[e.Detector]; on {
				evaluations[e.Detector] = append(evaluations[e.Detector], e.At)
			} else if at, gone := retiredAt[e.Detector]; gone && e.At.Sub(at) > retiredBound[e.Detector] {
				return violate(PropDetectionRuns, i, "detector %q was removed by the revision activated at %s but still evaluated at %s", e.Detector, at.Format(time.RFC3339Nano), e.At.Format(time.RFC3339Nano))
			}
		}
	}
	var remaining []string
	for d := range activeSince {
		remaining = append(remaining, d)
	}
	sort.Strings(remaining)
	for _, d := range remaining {
		if err := closePeriod(d, end, -1); err != nil {
			return err
		}
	}
	return nil
}

func downtimeBetween(downs [][2]time.Time, from, to time.Time) time.Duration {
	var total time.Duration
	for _, d := range downs {
		lo, hi := d[0], d[1]
		if lo.Before(from) {
			lo = from
		}
		if hi.After(to) {
			hi = to
		}
		if hi.After(lo) {
			total += hi.Sub(lo)
		}
	}
	return total
}
