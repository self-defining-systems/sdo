package runtime

import (
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type sourceDiffView struct {
	program.ReadView
	diff program.Diff
}

func (v sourceDiffView) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) { return v.diff, nil }

// A deleted object that accepted source never declared is recovered, whatever
// its kind: the exact source absence plus live absence is the proof.
func TestDeletedUnacceptedObjectRetiresItsAbsentFromSourceFinding(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "synthetic-policy"}
	event := sdk.DriftEvent{Ref: ref, Source: "absent from accepted source", Classification: sdk.DriftUnexplained}
	finding := sdk.Finding{DetectorID: "drift.unexplained", RuleID: "unexplained", Status: sdk.FindingActive, PrimaryResource: ref,
		Metadata: map[string]any{"drift_event": event, "source_inventory": true}}
	for name, test := range map[string]struct {
		diff program.Diff
		want int
	}{
		"object gone, source absent": {program.Diff{SourceAbsent: true}, 0},
		"object still live":          {program.Diff{SourceAbsent: true, Changed: true}, 1},
	} {
		t.Run(name, func(t *testing.T) {
			got := retireDisappearedRecoveryEffects(sourceDiffView{diff: test.diff}, []sdk.Finding{finding})
			if len(got) != test.want {
				t.Fatalf("remaining findings = %d, want %d", len(got), test.want)
			}
		})
	}
}
