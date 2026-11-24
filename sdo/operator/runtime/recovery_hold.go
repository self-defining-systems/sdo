package runtime

import (
	"slices"
	"sort"
	"strings"
	"time"
)

// clearStreaks maps each detector to the start of its current clear streak.
// Closure keeps one over the controller's evaluations (persisted), the
// automatic stage one over its own observations; both feed recoveryHeld.
type clearStreaks map[string]time.Time

// track records one evaluation: clear starts (or continues) the detector's
// streak, anything else ends it. An observer that knows an evaluation produced
// no observation at all (see noObservation) does not call it.
func (s clearStreaks) track(detectorID string, now time.Time, clear bool) {
	if !clear {
		delete(s, detectorID)
		return
	}
	if _, ok := s[detectorID]; !ok {
		s[detectorID] = now.UTC()
	}
}

// recoveryHeld is Recovered held through the stability window: the one
// decision that credits recovery, closes an incident and lets the automatic
// stage skip the responder. Closure and the stage both call it.
//
// It holds at now when nothing blocks it (blocked is empty: no alert-set
// finding open, the shared recovered() predicate verified, no program still
// diagnosing its fault), every detector in states (the alert set's detectors,
// recoveryDetectorSet) was evaluated clear (a missing, pending, failed or
// unavailable evaluation is not clear) with a clear streak, and that has been
// true at every observation since *run started, at least window ago. from is
// the earliest the run may count from (SDO's last write settling); a detector's
// clear streak that started later moves it forward. A blocked observation
// restarts the run at now; a detector not evaluated clear ends it (its streak
// decides where the next one starts).
func recoveryHeld(run *time.Time, now time.Time, window time.Duration, states []DetectorEvaluation, streaks clearStreaks, from time.Time, blocked string) (bool, string) {
	if blocked != "" {
		// The window restarts: it can start no earlier than now.
		*run = now
		return false, blocked
	}
	for _, state := range states {
		clear, ok := streaks[state.DetectorID]
		if !ok || state.Status != DetectorEvaluationClear {
			*run = time.Time{}
			return false, "detector " + state.DetectorID + " is not evaluated clear (" + string(state.Status) + ")"
		}
		if clear.After(from) {
			from = clear
		}
	}
	if run.IsZero() || from.After(*run) {
		*run = from
	}
	if now.Before(run.Add(window)) {
		return false, "recovery has not yet held through the stability window"
	}
	return true, ""
}

// recoveryDetectorSet is the set of detectors Recovered waits on: every
// evaluated detector (known) that owns a member of the incident's alert set
// (opening alerts and joined user tests). An alert set that names none waits on
// every health detector; without those (legacy bundles) on the detectors of
// the request's findings.
func recoveryDetectorSet(request IncidentRequest, known, health []string) []string {
	var ids []string
	for _, key := range alertKeys(&request) {
		if id, _, ok := strings.Cut(key, "\x00"); ok && slices.Contains(known, id) {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		ids = append(ids, health...)
	}
	if len(ids) == 0 {
		for _, finding := range request.Findings {
			if !strings.HasPrefix(finding.DetectorID, "drift.") {
				ids = append(ids, finding.DetectorID)
			}
		}
	}
	sort.Strings(ids)
	return slices.Compact(ids)
}

// alertSetStates are the latest evaluations of ids among states; a detector
// with none is reported as not evaluated, which recoveryHeld never calls clear.
func alertSetStates(ids []string, states []DetectorEvaluation) []DetectorEvaluation {
	result := make([]DetectorEvaluation, 0, len(ids))
	for _, id := range ids {
		found := DetectorEvaluation{DetectorID: id, Status: DetectorEvaluationPending}
		for _, state := range states {
			if state.DetectorID == id {
				found = state
			}
		}
		result = append(result, found)
	}
	return result
}
