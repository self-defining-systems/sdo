package sdktest

import (
	"encoding/json"
	"reflect"
	"regexp"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

var serviceTargetPortPath = regexp.MustCompile(`^spec\.ports\[[0-9]+\]\.targetPort$`)

// Service targetPort writes cannot alter a direct TCP observation of an unchanged
// Pod. Preserve only those original observations as inputs to projected diagnosis;
// never reuse Service probes, fixed answers, or reads after other kinds of edits.
func unchangedPodTCPRecordings(c CorpusCase, plan program.Plan, projected map[string]any) []PrimitiveCall {
	for _, action := range plan.Actions {
		field, ok := action.(program.SetField)
		if !ok {
			if pointer, yes := action.(*program.SetField); yes && pointer != nil {
				field = *pointer
				ok = true
			}
		}
		if !ok || field.Ref.Kind != "Service" || !serviceTargetPortPath.MatchString(field.Path) {
			return nil
		}
	}
	data, err := json.Marshal(c.Open)
	if err != nil {
		return nil
	}
	var original map[string]any
	if json.Unmarshal(data, &original) != nil {
		return nil
	}
	var result []PrimitiveCall
	for _, call := range c.PhaseRecordings("open") {
		if call.Method != "ProbeTCP" {
			continue
		}
		var args []json.RawMessage
		if json.Unmarshal(call.Arguments, &args) != nil || len(args) != 2 {
			continue
		}
		var ref sdk.ObjectRef
		if json.Unmarshal(args[0], &ref) != nil || ref.Kind != "Pod" {
			continue
		}
		before, err := fixtureObject(original, ref)
		if err != nil {
			continue
		}
		after, err := fixtureObject(projected, ref)
		if err != nil || !reflect.DeepEqual(before, after) {
			continue
		}
		result = append(result, call)
	}
	return result
}
