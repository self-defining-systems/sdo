package validate

import (
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestReflectionDecisionContractRequiresConsistentSDKGaps(t *testing.T) {
	gap := &contracts.SdkGap{FaultClass: "catalog", Primitive: "catalog read", Reason: "unavailable", ScratchScript: "inspect catalog"}
	for _, test := range []struct {
		name     string
		decision *contracts.ReflectionDecision
		want     bool
	}{
		{"missing gap", &contracts.ReflectionDecision{LearningDecision: "sdk_gap", NoChangeReason: "unavailable"}, true},
		{"no_change with gap", &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "unavailable", SdkGaps: []*contracts.SdkGap{gap}}, true},
		{"valid sdk gap", &contracts.ReflectionDecision{LearningDecision: "sdk_gap", NoChangeReason: "unavailable", SdkGaps: []*contracts.SdkGap{gap}}, false},
		{"valid no change", &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "already covered"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			diagnostics := ReflectionDecisionContractDiagnostics(test.decision)
			if (len(diagnostics) > 0) != test.want {
				t.Fatalf("diagnostics=%v", diagnostics)
			}
			for _, diagnostic := range diagnostics {
				if diagnostic.Rule != RuleReflectionDecision {
					t.Fatalf("unexpected rule: %v", diagnostic)
				}
			}
		})
	}
}

func TestReflectionResultRejectsDeprecatedPayloadBeforeProjection(t *testing.T) {
	result := &contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered", ArtifactProposal: &contracts.OperationalArtifactProposal{}}
	diagnostics := ReflectionResultDiagnostics(result)
	if len(diagnostics) != 1 || diagnostics[0].Rule != RuleReflectionDecision {
		t.Fatalf("deprecated payload escaped: %v", diagnostics)
	}
}
