package runtime

import (
	"fmt"
	"reflect"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

<<<<<<< HEAD
// checkRetainedDrift never infers clearance from missing namespace bindings or
// quiet health. Each retained learned binding must explicitly return NoMatch.
func checkRetainedDrift(ctx context.Context, retained []sdk.Finding, view program.ReadView, registry program.ProgramRegistry, deadline time.Duration, benign func(sdk.DriftEvent) bool, now time.Time) ([]sdk.Finding, []DetectorEvaluation, []ProgramRun) {
	unresolved := []sdk.Finding{}
	states := map[string]DetectorEvaluation{}
	runs := []ProgramRun{}
=======
// retainedDrift rechecks unexplained drift against accepted source and returns
// the findings whose declared fields still differ. It is information for the
// responder and the outcome; it never decides health or closure.
func retainedDrift(retained []sdk.Finding, view program.ReadView) []sdk.Finding {
	remaining := []sdk.Finding{}
>>>>>>> origin/vic-lsh/dev/sdo-improve
	for _, finding := range retained {
		if finding.DetectorID != "drift.unexplained" {
			continue
		}
<<<<<<< HEAD
		state, exists := states[finding.DetectorID]
		if !exists {
			state = DetectorEvaluation{DetectorID: finding.DetectorID, EvaluatedAt: now.UTC(), Status: DetectorEvaluationClear}
=======
		events := driftEventsForFindings(nil, []sdk.Finding{finding})
		diff, err := declaredSourceDiff(view, finding.PrimaryResource)
		if err != nil || !sourceDriftRestored(diff, events) {
			remaining = append(remaining, finding)
>>>>>>> origin/vic-lsh/dev/sdo-improve
		}
	}
	return remaining
}

func driftDiffValues(diff program.Diff) (map[string]any, map[string]any, bool) {
	accepted, observed, err := diff.Sides()
	if err != nil || accepted == nil || observed == nil {
		return nil, nil, false
	}
	return driftComparisonValues(accepted), driftComparisonValues(observed), true
}
func driftFieldsRestored(diff program.Diff, events []sdk.DriftEvent) bool {
	accepted, observed, ok := driftDiffValues(diff)
	if !ok || diff.SourceAbsent || len(events) != 1 || len(events[0].Fields) == 0 {
		return false
	}
	for _, field := range events[0].Fields {
		left, leftExists := accepted[field.Path]
		right, rightExists := observed[field.Path]
		// Equal absence is restoration when both complete objects are known.
		if leftExists != rightExists || !reflect.DeepEqual(left, right) {
			return false
		}
	}
	return true
}

// Status and independent closure share the same complete source comparison.
// Apply-normalized Changed alone cannot prove that admitted fields recovered.
func sourceDriftRestored(diff program.Diff, events []sdk.DriftEvent) bool {
	if diff.SourceAbsent {
		return !diff.Changed
	}
	return driftFieldsRestored(diff, events) || (!diff.Changed && diff.Text == "")
}

// Preserve containers as well as leaves so an aggregate is compared as an
// actual value, while genuine absence remains distinct from an absent object.
func driftComparisonValues(object map[string]any) map[string]any {
	values := map[string]any{}
	var walk func(string, any)
	walk = func(path string, value any) {
		values[path] = value
		switch value := value.(type) {
		case map[string]any:
			for key, child := range value {
				walk(path+"."+key, child)
			}
		case []any:
			for index, child := range value {
				walk(fmt.Sprintf("%s[%d]", path, index), child)
			}
		}
	}
<<<<<<< HEAD
	healthQuiet := true
	for _, id := range c.healthDetectorIDs {
		states, clear := c.latestEvaluations([]string{id}, time.Time{}, true)
		healthQuiet = healthQuiet && clear && len(states) > 0
	}
	benign := func(event sdk.DriftEvent) bool {
		if !healthQuiet {
			return false
		}
		for _, label := range assessed {
			if label.InvestigationVerdict == "benign" && label.InvestigationStatus == "completed" && driftEventKey(label) == driftEventKey(event) && sameDriftObservation(label, event) {
				return true
			}
		}
		return false
	}
	active, states, runs := checkRetainedDrift(ctx, retained, c.drift.View(snapshot), c.drift.Programs, c.drift.Deadline, benign, now)
	c.observeProgramRuns(now, nil, runs)
	for _, state := range states {
		if state.DetectorID != "drift.unexplained" {
			continue
		}
		findings := []sdk.Finding{}
		for _, finding := range active {
			if finding.DetectorID == state.DetectorID {
				findings = append(findings, finding)
			}
		}
		c.tracker.SetPolicy(state.DetectorID, 1, 1, 0)
		changes := c.tracker.Observe(now, state.DetectorID, findings)
		c.batcher.RemoveKeys(changes.Cleared)
		// A pending batch may predate tracker restoration. Retract its freshly
		// cleared keys too, rather than resurrecting historical drift on baseline.
		keys := []string{}
		for _, f := range retained {
			if f.DetectorID != state.DetectorID {
				continue
			}
			keep := false
			for _, a := range findings {
				keep = keep || FindingFingerprint(a) == FindingFingerprint(f)
			}
			if !keep {
				keys = append(keys, FindingStateKey(f.DetectorID, FindingFingerprint(f)))
			}
		}
		c.batcher.RemoveKeys(keys)
		if state.Status == DetectorEvaluationError {
			c.recordDetectorError(state.DetectorID, now, fmt.Errorf("%s", state.Error))
		} else {
			c.recordEvaluation(state.DetectorID, now, findings)
		}
		c.recordFiring(sdk.DetectorSpec{ID: state.DetectorID, Class: sdk.DetectorClassIncident}, now, findings, changes)
=======
	for key, value := range comparableFields(object) {
		walk(key, value)
>>>>>>> origin/vic-lsh/dev/sdo-improve
	}
	return values
}

func hasRetainedDrift(retained []sdk.Finding) bool {
	for _, finding := range retained {
		if finding.DetectorID == "drift.unexplained" {
			return true
		}
	}
	return false
}
