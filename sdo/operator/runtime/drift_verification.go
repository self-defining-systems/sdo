package runtime

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
)

// retainedDrift rechecks unexplained drift against accepted source and returns
// the findings whose declared fields still differ. It is information for the
// responder and the outcome; it never decides health or closure.
func retainedDrift(retained []sdk.Finding, view program.ReadView) []sdk.Finding {
	remaining := []sdk.Finding{}
	for _, finding := range retained {
		if finding.DetectorID != "drift.unexplained" {
			continue
		}
		events := driftEventsForFindings(nil, []sdk.Finding{finding})
		diff, err := declaredSourceDiff(view, finding.PrimaryResource)
		if err != nil || !sourceDriftRestored(diff, events) {
			remaining = append(remaining, finding)
		}
	}
	return remaining
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
	if json.Unmarshal([]byte(parts[0]), &accepted) != nil || json.Unmarshal([]byte(parts[1]), &observed) != nil || accepted == nil || observed == nil {
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
	for key, value := range comparableFields(object) {
		walk(key, value)
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
