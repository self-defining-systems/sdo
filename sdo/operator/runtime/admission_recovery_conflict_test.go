package runtime

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/workloadhealth"
)

// API bookkeeping advances even when a patch leaves configuration unchanged.
type admissionReadbackClient struct{ TypedPlanClient }

func (c admissionReadbackClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, data []byte) error {
	if err := c.TypedPlanClient.Patch(ctx, ref, kind, data); err != nil {
		return err
	}
	if ref.Kind == "ReplicaSet" {
		set, err := c.Client.AppsV1().ReplicaSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		set.ResourceVersion += "-next"
		_, err = c.Client.AppsV1().ReplicaSets(ref.Namespace).Update(ctx, set, metav1.UpdateOptions{})
		return err
	}
	return nil
}

// Feed the real admission detector into the stage candidate consumer. Program
// names and coverage order convey no dependency; the typed finding does.
func TestCoveredAdmissionPrerequisitePrecedesWorkloadRepairs(t *testing.T) {
	for _, id := range []string{"aaa-repair", "zzz-repair"} {
		detector := workloadhealth.NewPodAdmissionDetector(sdk.DetectorSpec{ID: "renamed-detector", Playbooks: []string{id}})
		findings, err := detector.Detect(context.Background(), sdktest.Snapshot{NamespaceName: "app", Admission: &sdk.PodAdmissionResult{Namespace: "app", Rejection: `failed calling webhook "arbitrary.example": x509: certificate signed by unknown authority`}})
		if err != nil || len(findings) != 1 {
			t.Fatalf("admission findings: %v %v", findings, err)
		}
		blocked := sdk.Finding{DetectorID: "templates", RuleID: "drift", Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}, Playbooks: []string{"template"}}
		findings = append([]sdk.Finding{blocked}, findings...)
		runs := []ProgramRun{}
		matches := []*contracts.PlaybookMatch{}
		for index, name := range []string{"template", id} {
			runs = append(runs, ProgramRun{ProgramID: name, FindingFingerprint: FindingFingerprint(findings[index]), Bindings: program.Bindings{}, Verdict: program.Verdict{Result: program.Match}})
			matches = append(matches, &contracts.PlaybookMatch{PlaybookId: name})
		}
		state := &IncidentState{Request: IncidentRequest{Findings: findings, InitialProgramRuns: runs}, Findings: findings, Coverage: &contracts.PlaybookCoverage{Matches: matches}}
		got := stageCandidates(state)
		if len(got) != 2 || got[0].PlaybookId != id {
			t.Fatalf("admission prerequisite was not first: %v", got)
		}
	}
}

