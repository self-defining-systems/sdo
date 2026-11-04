package runtime

import (
	"fmt"
	"reflect"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

// stageAlreadyAttempted skips a match whose writes all repeat writes an
// earlier attempt of this stage applied while that attempt's effect is still
// in progress. Writing again (a second restart, say) would supersede the
// earlier application and take its recovery credit without adding evidence.
// The value is the existing StageMitigation.skip_reason contract label.
const stageAlreadyAttempted = "already_attempted"

// sameWriteInProgress reports whether every write of plan repeats a write of
// an earlier attempt in report whose effect has not settled yet. The caller
// holds the loop mutex.
func (l *ResponderLoop) sameWriteInProgress(report *contracts.StageReport, plan program.Plan) bool {
	writes := planWrites(plan)
	if len(writes) == 0 {
		return false
	}
	var inProgress []program.Action
	for _, earlier := range report.GetMitigations() {
		if !stageWriteInProgress(earlier) {
			continue
		}
		for _, application := range l.applicationsLog {
			if application.ID == earlier.GetApplyReceipt().GetApplicationId() {
				inProgress = append(inProgress, planWrites(attemptApplication(application, earlier).Result.Plan)...)
			}
		}
	}
	for _, write := range writes {
		repeated := false
		for _, applied := range inProgress {
			repeated = repeated || sameWrite(write, applied)
		}
		if !repeated {
			return false
		}
	}
	return true
}

// stageWriteInProgress is an applied, unreverted attempt that neither failed
// nor settled, and whose own diagnosis no longer matches: its rollout or the
// recovery it waits for is still under way.
func stageWriteInProgress(attempt *contracts.StageMitigation) bool {
	receipt := attempt.GetApplyReceipt()
	unsettled := attempt.GetResult() == "rollout_pending_at_handoff" || attempt.GetResult() == "applied_still_matching"
	return unsettled && receipt.GetApplied() && !receipt.GetRolledBack() &&
		attempt.GetPostApplyDiagnose().GetVerdict().GetResult() == contracts.Verdict_RESULT_NO_MATCH
}

// planWrites are the plan's actions that change cluster state.
func planWrites(plan program.Plan) []program.Action {
	var writes []program.Action
	for _, action := range plan.Actions {
		if _, wait := generalAction(action).(program.WaitFor); wait {
			continue
		}
		writes = append(writes, action)
	}
	return writes
}

// sameWrite reports whether two actions make the same change: the same kind of
// action on the same object with the same arguments and the same resolved
// values. Reason narrates a write and is not part of it.
func sameWrite(a, b program.Action) bool {
	left, right := writeIdentity(reflect.ValueOf(a), true), writeIdentity(reflect.ValueOf(b), true)
	return left != nil && right != nil && reflect.DeepEqual(left, right)
}

var programValueType = reflect.TypeOf(program.Value{})

// writeIdentity is a comparable tree of an action's arguments. It is nil when
// an argument cannot be compared (a function, an unresolvable value), so such
// a write is never the same as another.
func writeIdentity(value reflect.Value, action bool) any {
	if !value.IsValid() {
		return "<nil>"
	}
	if value.Type() == programValueType {
		if !value.CanInterface() {
			return nil
		}
		return valueIdentity(value.Interface().(program.Value))
	}
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface:
		if value.IsNil() {
			return "<nil>"
		}
		return writeIdentity(value.Elem(), action)
	case reflect.Struct:
		fields := map[string]any{"<type>": value.Type().String()}
		for i := 0; i < value.NumField(); i++ {
			name := value.Type().Field(i).Name
			if action && name == "Reason" {
				continue
			}
			field := writeIdentity(value.Field(i), false)
			if field == nil {
				return nil
			}
			fields[name] = field
		}
		return fields
	case reflect.Slice, reflect.Array:
		items := make([]any, value.Len())
		for i := range items {
			if items[i] = writeIdentity(value.Index(i), false); items[i] == nil {
				return nil
			}
		}
		return items
	case reflect.Map:
		entries := map[string]any{}
		for _, key := range value.MapKeys() {
			entry := writeIdentity(value.MapIndex(key), false)
			if entry == nil {
				return nil
			}
			entries[fmt.Sprint(writeIdentity(key, false))] = entry
		}
		return entries
	case reflect.Bool:
		return value.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return value.Uint()
	case reflect.Float32, reflect.Float64:
		return value.Float()
	case reflect.String:
		return value.String()
	}
	return nil
}

// valueIdentity names what a plan value writes. Accepted source is fixed for
// the incident, so its reference identifies it; a parameter or observed value
// is compared by the value it resolves to in its own program invocation.
func valueIdentity(value program.Value) any {
	if value.Origin() == program.SourceValue {
		return map[string]any{"origin": string(value.Origin()), "ref": writeIdentity(reflect.ValueOf(value.Ref()), false), "path": value.Path()}
	}
	resolved, _, err := program.ResolveValue(value, program.ValueInputs{})
	if err != nil {
		return nil
	}
	return map[string]any{"origin": string(value.Origin()), "value": writeIdentity(reflect.ValueOf(resolved), false)}
}
