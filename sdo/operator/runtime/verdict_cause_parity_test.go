package runtime

import (
	"slices"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk/program"
)

// A program verdict states its cause in the responder's vocabulary.
func TestVerdictCauseVocabularyMatchesConfirmedRootCause(t *testing.T) {
	sdk := program.CauseClasses()
	slices.Sort(sdk)
	for _, declared := range [][]string{wire.StringIn(&contracts.ConfirmedRootCause{}, "cause_class"), wire.StringIn(&contracts.Verdict{}, "cause_class")} {
		declared = slices.DeleteFunc(slices.Clone(declared), func(v string) bool { return v == "" })
		slices.Sort(declared)
		if !slices.Equal(declared, sdk) {
			t.Fatalf("program.CauseClasses %v differ from the proto rule %v", sdk, declared)
		}
	}
	reach := slices.DeleteFunc(wire.StringIn(&contracts.Verdict{}, "reach"), func(v string) bool { return v == "" })
	slices.Sort(reach)
	if !slices.Equal(reach, []string{program.ReachInScope, program.ReachOutOfReach}) {
		t.Fatalf("Verdict.reach rule %v", reach)
	}
}
