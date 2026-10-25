package runtime

import (
	"context"
	"encoding/json"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"reflect"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sort"
	"strings"
	"time"
)

// checkRetainedDrift rechecks unexplained drift against accepted source.
// Playbooks never create or clear detector findings.
func checkRetainedDrift(ctx context.Context, retained []sdk.Finding, view program.ReadView) ([]sdk.Finding, []DetectorEvaluation) {
	unresolved := []sdk.Finding{}
	states := map[string]DetectorEvaluation{}
	for _, finding := range retained {
		if finding.DetectorID != "drift.unexplained" {
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
	return unresolved, evaluations
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
		left, leftExists := accepted[field.Path]
		right, rightExists := observed[field.Path]
		if !leftExists && !rightExists {
			return false
		}
		if leftExists != rightExists || !reflect.DeepEqual(left, right) {
			return false
		}
	}
	return true
}
