package validate

import (
	"fmt"
	"go/token"
	"reflect"
	"slices"
	"strings"
	"unicode"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

// Available effects come from the installed wire vocabulary and observations
// from the real ReadView interface, so adding an SDK primitive closes the same
// class of stale SDK-gap claim without maintaining a parallel capability list.
func recordedRepairSDKGapDiagnostics(outcome *contracts.OutcomeRecord, gaps []*contracts.SdkGap) []Diagnostic {
	effects := map[string]bool{}
	fields := (&contracts.Action{}).ProtoReflect().Descriptor().Oneofs().ByName("kind").Fields()
	for index := 0; index < fields.Len(); index++ {
		effects[capabilityName(string(fields.Get(index).Name()))] = true
	}
	reads := map[string]string{}
	readView := reflect.TypeOf((*program.ReadView)(nil)).Elem()
	for index := 0; index < readView.NumMethod(); index++ {
		name := readView.Method(index).Name
		reads[capabilityName(name)] = name
	}
	var ds []Diagnostic
	for _, gap := range gaps {
		primitive := gap.GetPrimitive()
		if index := strings.LastIndex(primitive, "."); index >= 0 {
			primitive = primitive[index+1:]
		}
		if !token.IsIdentifier(primitive) {
			ds = append(ds, Diagnostic{Rule: RuleReflectionSDKGapUnavailable, Message: fmt.Sprintf("SDK gap %q must name an unavailable observation or effect, not free-text work left to encode", gap.GetPrimitive()), Hint: "use the typed SDK-gap fields for a specific missing capability; recorded actions still require executable steps"})
			continue
		}
		name := capabilityName(gap.GetPrimitive())
		observation := reads[name]
		if !effects[name] && observation == "" {
			continue
		}
		unavailable := observation != "" && (slices.ContainsFunc(outcome.GetProgramToolCalls(), func(call *contracts.ProgramToolCall) bool { return strings.Contains(call.GetError(), observation) }) ||
			slices.ContainsFunc(outcome.GetProgramRuns(), func(run *contracts.ProgramRun) bool { return strings.Contains(run.GetError(), observation) }))
		if !effects[name] && unavailable {
			continue
		}
		ds = append(ds, Diagnostic{Rule: RuleReflectionSDKGapUnavailable, Message: fmt.Sprintf("SDK gap %q names an installed capability, not an unavailable observation or effect", gap.GetPrimitive()), Hint: "encode recorded actions as typed SDK or RunScript steps; not encoded yet is not an SDK gap. Name the actual unavailable capability and its observed limitation"})
	}
	return ds
}

func capabilityName(value string) string {
	if index := strings.LastIndex(value, "."); index >= 0 {
		value = value[index+1:]
	}
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}
		return -1
	}, value)
}
