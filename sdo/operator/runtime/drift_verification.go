package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"reflect"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sort"
	"strings"
	"time"
)

// checkRetainedDrift never infers clearance from missing namespace bindings or
// quiet health. Each retained learned binding must explicitly return NoMatch.
func checkRetainedDrift(ctx context.Context, retained []sdk.Finding, view program.ReadView, registry program.ProgramRegistry, deadline time.Duration, benign func(sdk.DriftEvent) bool) ([]sdk.Finding, []DetectorEvaluation, []ProgramRun) {
	unresolved := []sdk.Finding{}
	states := map[string]DetectorEvaluation{}
	runs := []ProgramRun{}
	for _, finding := range retained {
		if !strings.HasPrefix(finding.DetectorID, "drift.") {
			continue
		}
		state, exists := states[finding.DetectorID]
		if !exists {
			state = DetectorEvaluation{DetectorID: finding.DetectorID, EvaluatedAt: time.Now().UTC(), Status: DetectorEvaluationClear}
		}
		clear := false
		var err error
		if finding.DetectorID == "drift.unexplained" {
			events := driftEventsForFindings(nil, []sdk.Finding{finding})
			var diff program.Diff
			diff, err = view.DiffAgainstSource(finding.PrimaryResource)
			if err == nil {
				clear = !diff.Changed || driftFieldsRestored(diff, events)
				if !clear && len(events) == 1 && benign != nil && benign(events[0]) && driftObservationCurrent(diff, events[0]) {
					clear = true
				}
			}
		} else {
			id := strings.TrimPrefix(finding.DetectorID, "drift.")
			if registry != nil {
				if p, ok := registry.Diagnoser(id); ok {
					verdict := program.RunDiagnose(ctx, p, view, finding.ParameterBindings, deadline)
					runs = append(runs, ProgramRun{ProgramID: id, Bindings: finding.ParameterBindings, Verdict: verdict})
					clear = verdict.Result == program.NoMatch
				}
			}
		}
		if !clear {
			unresolved = append(unresolved, finding)
			state.Status = DetectorEvaluationFiring
			state.Fingerprints = append(state.Fingerprints, FindingFingerprint(finding))
			if err != nil {
				state.Status = DetectorEvaluationError
				state.Error = err.Error()
			}
		}
		states[finding.DetectorID] = state
	}
	evaluations := make([]DetectorEvaluation, 0, len(states))
	for _, state := range states {
		evaluations = append(evaluations, state)
	}
	sort.Slice(evaluations, func(i, j int) bool { return evaluations[i].DetectorID < evaluations[j].DetectorID })
	return unresolved, evaluations, runs
}

func driftDiffValues(diff program.Diff) (map[string]any, map[string]any, bool) {
	if !strings.HasPrefix(diff.Text, "accepted: ") {
		return nil, nil, false
	}
	parts := strings.SplitN(strings.TrimPrefix(diff.Text, "accepted: "), "\nobserved: ", 2)
	if len(parts) != 2 {
		return nil, nil, false
	}
	var accepted, observed map[string]any
	if json.Unmarshal([]byte(parts[0]), &accepted) != nil || json.Unmarshal([]byte(parts[1]), &observed) != nil {
		return nil, nil, false
	}
	return flattenDriftObject(&unstructured.Unstructured{Object: accepted}), flattenDriftObject(&unstructured.Unstructured{Object: observed}), true
}
func driftFieldsRestored(diff program.Diff, events []sdk.DriftEvent) bool {
	accepted, observed, ok := driftDiffValues(diff)
	if !ok || diff.SourceAbsent || len(events) != 1 || len(events[0].Fields) == 0 {
		return false
	}
	for _, field := range events[0].Fields {
		if !reflect.DeepEqual(accepted[field.Path], observed[field.Path]) {
			return false
		}
	}
	return true
}
func driftObservationCurrent(diff program.Diff, event sdk.DriftEvent) bool {
	// The successful source comparison must still describe precisely this event.
	if diff.SourceAbsent {
		parts := strings.SplitN(diff.Text, "\nobserved: ", 2)
		if len(parts) != 2 || len(event.Fields) != 1 || event.Fields[0].Path != "spec" {
			return false
		}
		var observed map[string]any
		var spec any
		return json.Unmarshal([]byte(parts[1]), &observed) == nil && json.Unmarshal([]byte(event.Fields[0].New), &spec) == nil && reflect.DeepEqual(observed["spec"], spec)
	}
	accepted, observed, ok := driftDiffValues(diff)
	if !ok {
		return false
	}
	for _, field := range event.Fields {
		if renderDriftValue(field.Path, accepted[field.Path]) != field.Source || renderDriftValue(field.Path, observed[field.Path]) != field.New {
			return false
		}
	}
	return len(event.Fields) > 0
}

func (c *Controller) refreshRetainedDrift(ctx context.Context, now time.Time, snapshot sdk.DetectionContext) {
	if c.drift == nil || c.drift.View == nil {
		return
	}
	c.mu.Lock()
	retained := append([]sdk.Finding(nil), c.batcher.Snapshot().Findings...)
	if c.currentIncidentRequest != nil {
		retained = append(retained, c.currentIncidentRequest.Findings...)
	}
	var assessed []sdk.DriftEvent
	if c.currentIncidentResult != nil {
		assessed = checkedDriftAssessments(*c.currentIncidentRequest, c.currentIncidentResult.DriftEvents)
	}
	c.mu.Unlock()
	// Accepted outcomes are the only persistent benign label store. A source
	// rollback therefore discards labels with the outcomes it rolls back.
	if c.config.RepositoryWorktree != "" {
		stored, err := memory.OpenReader(c.config.RepositoryWorktree).List(ctx)
		if err == nil {
			for _, outcome := range stored {
				assessed = append(assessed, driftEventsState(outcome.Record.GetDriftEvents())...)
			}
		}
	}
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
	active, states, runs := checkRetainedDrift(ctx, retained, c.drift.View(snapshot), c.drift.Programs, c.drift.Deadline, benign)
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
	}
}
