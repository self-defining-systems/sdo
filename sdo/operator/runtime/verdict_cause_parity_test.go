package runtime

import (
	"context"
	"slices"
	"testing"
	"time"

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

type causeLabelDiagnosis struct{ label string }

func (p causeLabelDiagnosis) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "cause-label"}
}

func (p causeLabelDiagnosis) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.Match, CauseClass: p.label, Reach: program.ReachOutOfReach}, nil
}

func TestFreeTextCauseKeepsMatchWithoutEscalating(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		label, category string
		escalates       bool
	}{{"free-form", "other", false}, {"other", "other", true}, {"network-delay", "network-delay", true}} {
		t.Run(tc.label, func(t *testing.T) {
			v := program.RunDiagnose(ctx, causeLabelDiagnosis{label: tc.label}, nil, program.Bindings{}, time.Second)
			_, escalates, err := stageOutOfReach(ctx, nil, false, nil, program.Bindings{}, v, time.Second)
			if err != nil || v.Result != program.Match || v.CauseClass != tc.category || escalates != tc.escalates {
				t.Fatalf("verdict = %+v, escalates = %v, err = %v", v, escalates, err)
			}
			raw, _ := (causeLabelDiagnosis{label: tc.label}).Diagnose(ctx, nil, program.Bindings{})
			_, escalates, err = stageOutOfReach(ctx, nil, false, nil, program.Bindings{}, raw, time.Second)
			if err != nil || escalates != tc.escalates {
				t.Fatalf("raw verdict escalates = %v, err = %v", escalates, err)
			}
		})
	}
}
