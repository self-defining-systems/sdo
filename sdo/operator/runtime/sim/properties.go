package sim

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
)

// The six behavioral properties of the controller, each a plain function over
// the Log. Agents, agent-written programs and the cluster are untrusted inputs:
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
	PropFixedMeansVerified = "fixed-means-verified"
	PropWriteAtMostOnce    = "write-at-most-once"
	PropEveryIncidentEnds  = "every-incident-ends"
	PropDetectionRuns      = "detection-keeps-running"
	PropNothingLost        = "nothing-is-lost"
	PropMemoryGetsUsed     = "saved-memory-gets-used"
)

// Config holds the run-specific bounds the checks need.
type Config struct {
	// MaxDetectionGap bounds the time between two evaluations of a detector.
	MaxDetectionGap time.Duration
	// Settle is the least time a run must last after its faults stop for every
	// incident to be expected to end.
	Settle time.Duration
}

// Check runs all six checks and returns every violation, in property order.
func Check(log Log, cfg Config) []error {
	var out []error
	for _, err := range []error{
		FixedMeansVerified(log), WriteAtMostOnce(log), EveryIncidentEnds(log, cfg),
		DetectionKeepsRunning(log, cfg), NothingIsLost(log), SavedMemoryGetsUsed(log),
	} {
		if err != nil {
			out = append(out, err)
		}
	}
	return out
}

// FixedMeansVerified is property 1.
//
// A finding is marked fixed (a Fixed entry) only from a verification observed
// after the last write that could affect it. Precisely, for each Fixed entry F
// at log index i:
//
//   - the observation F cites is an Observation recorded before i;
//   - an Evaluated entry of F's detector on that observation exists before i
//     and does not list F's finding as active;
//   - every Write before i that affects F satisfies W.Seq <= observation.ThroughSeq
//     (the observed view reflects the write; a stale informer cache fails this)
//     and W.At <= observation.At.
//
// A write affects F when it is committed, or attempted with an unknown reply,
// (Committed true; a write that provably did not reach the cluster is ignored)
// and its object is in F's Scope, where Scope lists the objects F's evidence
// depends on: its primary and related resources and anything the finding's
// detector reads for it (owners, selected pods, dependents). An empty Scope means
// the scope is unknown, so every committed write affects F. The write may belong
// to any incident or binding: affect is about the cluster, not the actor.
func FixedMeansVerified(log Log) error {
	observations := map[int]Observation{}
	for i, entry := range log {
		switch e := entry.(type) {
		case Observation:
			observations[e.ID] = e
		case Fixed:
			obs, ok := observations[e.Observation]
			if !ok {
				return violate(PropFixedMeansVerified, i, "finding %q marked fixed citing observation %d, which was not recorded before the mark", e.Finding.Key, e.Observation)
			}
			evaluated := false
			for _, prior := range log[:i] {
				ev, ok := prior.(Evaluated)
				if !ok || ev.Observation != e.Observation || ev.Detector != e.Finding.Detector {
					continue
				}
				evaluated = true
				for _, key := range ev.Active {
					if key == e.Finding.Key {
						return violate(PropFixedMeansVerified, i, "finding %q marked fixed, but observation %d still showed it active", e.Finding.Key, obs.ID)
					}
				}
			}
			if !evaluated {
				return violate(PropFixedMeansVerified, i, "finding %q marked fixed with no evaluation of detector %q on observation %d", e.Finding.Key, e.Finding.Detector, obs.ID)
			}
			for j, prior := range log[:i] {
				w, ok := prior.(Write)
				if !ok || !w.Committed || !affects(w, e.Scope) {
					continue
				}
				if w.Seq > obs.ThroughSeq || w.At.After(obs.At) {
					return violate(PropFixedMeansVerified, i, "finding %q marked fixed from observation %d (view through write %d at %s), but log[%d] wrote %s (write %d at %s) after it", e.Finding.Key, obs.ID, obs.ThroughSeq, obs.At.Format(time.RFC3339Nano), j, refString(w.Object), w.Seq, w.At.Format(time.RFC3339Nano))
				}
			}
		}
	}
	return nil
}

func affects(w Write, scope []sdk.ObjectRef) bool {
	if len(scope) == 0 {
		return true
	}
	for _, ref := range scope {
		if ref == w.Object {
			return true
		}
	}
	return false
}

func refString(ref sdk.ObjectRef) string {
	return ref.Kind + "/" + ref.Namespace + "/" + ref.Name
}

// WriteAtMostOnce is property 2.
//
// The same write for the same binding is never applied twice. A write's
// identity is (incident, binding, action index, object, patch digest); attempt
// numbers are not part of it, because a retry is the same write. Counting every
// committed write (including one whose reply was lost, which the caller cannot
// see), no identity occurs twice anywhere in the log, however many controller
// restarts, host crashes or retries lie between the occurrences. A committed
// write without a binding cannot be attributed and is itself a violation: every
// controller write must belong to a binding.
func WriteAtMostOnce(log Log) error {
	type identity struct {
		incident, binding string
		action            int
		object            sdk.ObjectRef
		patch             string
	}
	first := map[identity]int{}
	for i, entry := range log {
		w, ok := entry.(Write)
		if !ok || !w.Committed {
			continue
		}
		if w.Attribution.Binding == "" {
			return violate(PropWriteAtMostOnce, i, "committed write to %s has no binding, so it cannot be shown to apply once", refString(w.Object))
		}
		id := identity{w.Attribution.Incident, w.Attribution.Binding, w.Attribution.Action, w.Object, w.Patch}
		if j, dup := first[id]; dup {
			return violate(PropWriteAtMostOnce, i, "write to %s for binding %q (incident %q, action %d) applied again; first applied at log[%d], second is attempt %d%s", refString(w.Object), id.binding, id.incident, id.action, j, w.Attribution.Attempt, restartsBetween(log, j, i))
		}
		first[id] = i
	}
	return nil
}

func restartsBetween(log Log, from, to int) string {
	n := 0
	for _, entry := range log[from:to] {
		if _, ok := entry.(ControllerDown); ok {
			n++
		}
	}
	if n == 0 {
		return ""
	}
	return fmt.Sprintf(" (%d controller or host crash(es) between)", n)
}

// EveryIncidentEnds is property 3.
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

// DetectionKeepsRunning is property 4.
//
// Every detector in the active memory revision is evaluated often enough:
// for each detector d and each period during which d is active, the time
// between consecutive evaluations of d, between the start of the period and its
// first evaluation, and between its last evaluation and the end of the period,
// is at most cfg.MaxDetectionGap. d is active from an Activated entry that lists
// it until the next Activated entry that does not (or RunEnded); a revision
// swap, incident closure and agent work therefore all count against the bound.
// Time while the controller is down (ControllerDown to the next ControllerUp,
// or the end of the run) is not counted: nothing runs then, and the restart's
// own recovery time is bounded by the same gap once it is up. The first
// Activated entry must precede every Evaluated entry.
func DetectionKeepsRunning(log Log, cfg Config) error {
	if cfg.MaxDetectionGap <= 0 {
		return violate(PropDetectionRuns, -1, "detection gap bound is not configured")
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
	evaluations := map[string][]time.Time{}
	closePeriod := func(d string, until time.Time, at int) error {
		start := activeSince[d]
		points := append([]time.Time{start}, evaluations[d]...)
		points = append(points, until)
		for k := 1; k < len(points); k++ {
			if gap := points[k].Sub(points[k-1]) - downtimeBetween(downs, points[k-1], points[k]); gap > cfg.MaxDetectionGap {
				return violate(PropDetectionRuns, at, "detector %q was not evaluated for %s (limit %s) between %s and %s", d, gap, cfg.MaxDetectionGap, points[k-1].Format(time.RFC3339Nano), points[k].Format(time.RFC3339Nano))
			}
		}
		delete(activeSince, d)
		delete(evaluations, d)
		return nil
	}
	activated := false
	for i, entry := range log {
		switch e := entry.(type) {
		case Activated:
			activated = true
			listed := map[string]bool{}
			for _, d := range e.Detectors {
				listed[d] = true
			}
			var drop []string
			for d := range activeSince {
				if !listed[d] {
					drop = append(drop, d)
				}
			}
			sort.Strings(drop)
			for _, d := range drop {
				if err := closePeriod(d, e.At, i); err != nil {
					return err
				}
			}
			for _, d := range e.Detectors {
				if _, on := activeSince[d]; !on {
					activeSince[d] = e.At
				}
			}
		case Evaluated:
			if !activated {
				return violate(PropDetectionRuns, i, "detector %q evaluated before any memory revision was activated", e.Detector)
			}
			if _, on := activeSince[e.Detector]; on {
				evaluations[e.Detector] = append(evaluations[e.Detector], e.At)
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

// NothingIsLost is property 5.
//
// A fault first seen during an incident stays tracked after that incident
// closes. A Seen entry with a non-empty Incident I is such a fault: it
// activated while I's responder was running, so it is not part of I's opening
// batch. Let tc be I's terminal Outcome (Closed or Failed). From tc until the
// finding's next Cleared entry (the detector observed it absent for its clear
// threshold; if none, to the end of the log), every Tracked snapshot at or after
// tc must contain its key. At least one Tracked snapshot at or after tc must
// exist, otherwise the survival is unobserved and the check reports that.
// Tracking is the controller's finding-state record of active findings; a
// finding handed to a new incident, or still active and waiting, both count as
// tracked, because both stay in that record.
func NothingIsLost(log Log) error {
	closedAt := map[string]time.Time{}
	for _, entry := range log {
		if o, ok := entry.(Outcome); ok {
			if _, done := closedAt[o.Incident]; !done {
				closedAt[o.Incident] = o.At
			}
		}
	}
	for i, entry := range log {
		s, ok := entry.(Seen)
		if !ok || s.Incident == "" {
			continue
		}
		tc, closed := closedAt[s.Incident]
		if !closed {
			continue
		}
		cleared := false
		snapshots := 0
		for j := i + 1; j < len(log); j++ {
			switch e := log[j].(type) {
			case Cleared:
				if e.Finding.Key == s.Finding.Key {
					cleared = true
				}
			case Tracked:
				if cleared || e.At.Before(tc) {
					continue
				}
				snapshots++
				if !contains(e.Keys, s.Finding.Key) {
					return violate(PropNothingLost, j, "finding %q first seen at %s during incident %q is no longer tracked at %s, after the incident ended at %s, and was never cleared", s.Finding.Key, s.At.Format(time.RFC3339Nano), s.Incident, e.At.Format(time.RFC3339Nano), tc.Format(time.RFC3339Nano))
				}
			}
			if cleared {
				break
			}
		}
		if snapshots == 0 && !cleared {
			return violate(PropNothingLost, i, "finding %q first seen during incident %q has no tracked-set snapshot after the incident ended at %s", s.Finding.Key, s.Incident, tc.Format(time.RFC3339Nano))
		}
	}
	return nil
}

func contains(keys []string, key string) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}

// SavedMemoryGetsUsed is property 6.
//
// After a memory revision R is active (its Activated entry), the next incident
// that a program in R matches is handled by that program before any agent
// starts, without a controller restart. Precisely, for each Activated entry A
// with revision R, let I be the first Opened entry after A such that no
// ControllerDown lies between A and I (a restart in between would load R
// anyway and mask the defect, so such an incident is not a witness) and some
// program P in R matches I.
// P matches I when one of P's declared detectors owns a finding in I's opening
// batch. Then, before the first Handler of kind agent for I:
//
//   - a Handler of kind program for I with ID in the matching programs and
//     revision R must exist; or
//   - every matching program has a ProgramDiagnosed entry for I at revision R
//     with result no_match or error (a program that declines is allowed to be
//     skipped).
//
// An incident that never starts any handler is not a violation of this property
// (property 3 reports it). The next Activated entry ends A's window: an
// incident opened after it belongs to the new revision.
func SavedMemoryGetsUsed(log Log) error {
	for ai, entry := range log {
		act, ok := entry.(Activated)
		if !ok {
			continue
		}
	window:
		for oi := ai + 1; oi < len(log); oi++ {
			switch e := log[oi].(type) {
			case Activated, ControllerDown:
				break window
			case Opened:
				matching := matchingPrograms(act, e)
				if len(matching) == 0 {
					continue
				}
				if err := programBeforeAgent(log, oi, act, e, matching); err != nil {
					return err
				}
				break window
			}
		}
	}
	return nil
}

func matchingPrograms(act Activated, incident Opened) []string {
	detectors := map[string]bool{}
	for _, f := range incident.Findings {
		detectors[f.Detector] = true
	}
	var matching []string
	for _, p := range act.Programs {
		for _, d := range p.Detectors {
			if detectors[d] {
				matching = append(matching, p.ID)
				break
			}
		}
	}
	return matching
}

func programBeforeAgent(log Log, openedAt int, act Activated, incident Opened, matching []string) error {
	declined := map[string]bool{}
	for i := openedAt + 1; i < len(log); i++ {
		switch e := log[i].(type) {
		case ProgramDiagnosed:
			if e.Incident == incident.Incident && e.Revision == act.Revision && (e.Result == NoMatch || e.Result == DiagError) {
				declined[e.Program] = true
			}
		case Handler:
			if e.Incident != incident.Incident {
				continue
			}
			if e.Kind == ByProgram && e.Revision == act.Revision && contains(matching, e.ID) {
				return nil
			}
			if e.Kind == ByAgent {
				all := true
				for _, p := range matching {
					all = all && declined[p]
				}
				if all {
					return nil
				}
				return violate(PropMemoryGetsUsed, i, "incident %q matches saved program(s) %s of revision %q (active since %s), but an agent started before any of them handled it", incident.Incident, strings.Join(matching, ", "), act.Revision, act.At.Format(time.RFC3339Nano))
			}
		}
	}
	return nil
}
