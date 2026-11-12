package runtime

import (
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// The stage's cause is the incident's primary finding that the applied
// playbook addressed, never an unrelated finding that merely cleared beside it,
// and it exists whenever a program applied, cleared or not.
func TestStageCauseComesFromTheFindingTheAppliedPlaybookAddressed(t *testing.T) {
	api := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	policy := sdk.ObjectRef{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "synthetic-policy"}
	// The unrelated drift sorts first in the request.
	unrelated := sdk.Finding{DetectorID: "drift.unexplained", RuleID: "unexplained", Status: sdk.FindingActive, Summary: "unrelated drift", PrimaryResource: policy}
	primary := sdk.Finding{DetectorID: "api-stall", RuleID: "api-stall", Status: sdk.FindingActive, Summary: "api requests stall", PrimaryResource: api, Playbooks: []string{"api-restart"}}
	for name, test := range map[string]struct {
		result    string
		cleared   []string
		remaining []string
	}{
		"cleared":              {"applied_cleared", []string{FindingFingerprint(unrelated), FindingFingerprint(primary)}, nil},
		"clearance pending":    {"applied_still_matching", []string{FindingFingerprint(unrelated)}, []string{FindingFingerprint(primary)}},
		"nothing credited yet": {"applied_still_matching", nil, []string{FindingFingerprint(unrelated), FindingFingerprint(primary)}},
	} {
		t.Run(name, func(t *testing.T) {
			attempt := &contracts.StageMitigation{PlaybookId: "api-restart", Result: test.result,
				Bindings:                     []*contracts.Binding{{Name: "deployment", Object: objectRefContract(api)}},
				ApplyReceipt:                 &contracts.PlanApplication{Applied: true, ApplicationId: "application-1"},
				ClearedFindingFingerprints:   test.cleared,
				RemainingFindingFingerprints: test.remaining}
			state := &IncidentState{Request: IncidentRequest{IncidentID: "synthetic-cause", Findings: []sdk.Finding{unrelated, primary}},
				PriorStages: []*contracts.StageReport{{Stage: "deterministic", Mitigations: []*contracts.StageMitigation{attempt}}}}
			result, err := responsePipelineResult(state, time.Unix(0, 0), time.Unix(1, 0))
			if err != nil {
				t.Fatal(err)
			}
			if len(result.ConfirmedRootCauses) != 1 {
				t.Fatalf("causes = %v, want exactly one", result.ConfirmedRootCauses)
			}
			cause := result.ConfirmedRootCauses[0]
			if cause.Summary != "api requests stall" || len(cause.Resources) == 0 || cause.Resources[0] != api {
				t.Fatalf("cause = %+v, want the primary finding's summary and the repaired object first", cause)
			}
			for _, ref := range cause.Resources {
				if ref == policy {
					t.Fatalf("cause names the unrelated object: %+v", cause)
				}
			}
		})
	}
}
