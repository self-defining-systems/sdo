package runtime

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/outcome_derivation"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// This hand-written regression uses a toy Service and verifier, with real tool
// handling, ConfigMap persistence, outcome derivation and trust accounting.
func TestResponderFailedHealthSurvivesRestart(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "same_process"
		if restart {
			name = "restart"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			loop, kube := trustedPortLoop(t, false, false, false)
			dispatcher := KubernetesJobDispatcher{Client: kube, Namespace: "app"}
			attachRecoveryStore(t, dispatcher, loop)
			bindings := program.Bindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}
			first := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: bindings})
			if first.Error != "" {
				t.Fatal(first.Error)
			}
			applied := loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{first.PlanID}})
			if applied.Error != "" || loop.PlanApplications()[0].Result.VerificationPassed {
				t.Fatalf("fixture did not record a failed verification: %+v", applied)
			}
			if rolled := loop.Handle(ctx, ToolRequest{Command: "rollback", ApplicationID: applied.ApplicationID}); rolled.Error != "" || !loop.PlanApplications()[0].RolledBack {
				t.Fatalf("responder rollback: %+v", rolled)
			}
			service, err := kube.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{})
			if err != nil || service.Spec.Ports[0].TargetPort != intstr.FromInt32(9999) {
				t.Fatalf("rollback fixture: %v %v", service, err)
			}
			if restart {
				loop = restartPortLoop(loop)
				attachRecoveryStore(t, dispatcher, loop)
			}
			loop.Executor.Verifier = planFakeVerifier{passed: true}
			second := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: bindings})
			if second.Error != "" {
				t.Fatal(second.Error)
			}
			applied = loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{second.PlanID}})
			if applied.Error != "" || !applied.Healthy {
				t.Fatalf("retry failed: %+v", applied)
			}
			outcomes := []*contracts.OutcomeRecord{
				verifiedTrustOutcome(t, "service-targetport-mismatch", "tree-a"),
				verifiedTrustOutcome(t, "service-targetport-mismatch", "tree-a"),
				responderRetryOutcome(t, loop),
			}
			got := ProgramTrustStats(outcomes)["service-targetport-mismatch"]
			t.Logf("restart=%v applications=%d signals=%v loops=%d trust=%+v", restart, len(loop.PlanApplications()), loop.FailureSignals(), loop.LoopsUsed(), got)
			if got.Trusted || got.VerifiedSuccesses != 0 || got.Failures != 1 {
				t.Fatalf("failed verification must survive a successful retry: %+v", got)
			}
		})
	}
}

func TestResponderVerificationFailureTrustIsRevisionBound(t *testing.T) {
	outcomes := []*contracts.OutcomeRecord{
		verifiedTrustOutcome(t, "targetport", "revision-a"),
		verifiedTrustOutcome(t, "targetport", "revision-a"),
		{SourceCommit: "unrelated-source-commit", ProgramFailureSignals: []string{"application_verification_failure:targetport@revision-a"}},
		verifiedTrustOutcome(t, "targetport", "revision-a"),
	}
	if got := ProgramTrustStats(outcomes)["targetport"]; got.Trusted || got.Failures != 1 || got.LastChange != "revision-a" {
		t.Fatalf("successful retry erased revision failure: %+v", got)
	}
	for range 3 {
		outcomes = append(outcomes, verifiedTrustOutcome(t, "targetport", "revision-b"))
	}
	if got := ProgramTrustStats(outcomes)["targetport"]; !got.Trusted || got.Failures != 0 || got.LastChange != "revision-b" {
		t.Fatalf("failure affected a different revision: %+v", got)
	}
	outcomes = append(outcomes, verifiedTrustOutcome(t, "targetport", "revision-a"))
	if got := ProgramTrustStats(outcomes)["targetport"]; got.Trusted || got.Failures != 1 || got.LastChange != "revision-a" {
		t.Fatalf("reverting to the failed revision erased failure: %+v", got)
	}
}

func responderRetryOutcome(t *testing.T, loop *ResponderLoop) *contracts.OutcomeRecord {
	t.Helper()
	runs := loop.ProgramRuns()
	fillProgramRevisions(context.Background(), runs, loop.Request.RepositoryWorktree, loop.Request.SourceCommit)
	finding := loop.Request.Findings[0]
	message, err := incidentResultContract(IncidentResult{
		IncidentID: loop.Request.IncidentID, Status: IncidentCompleted,
		ProgramRuns: runs, ProgramToolCalls: loop.ToolCalls(),
		PlanApplications: loop.PlanApplications(), ProgramFailureSignals: loop.FailureSignals(),
		RepairActions: ProgramRepairReceipts(loop.PlanApplications(), nil),
		ConfirmedRootCauses: []ConfirmedRootCause{{
			Summary: "wrong service target port", Resources: []sdk.ObjectRef{finding.PrimaryResource},
			Evidence:           []RootCauseEvidence{{Kind: "detector-finding", Source: finding.DetectorID, Observation: finding.Evidence}},
			ExplainedDetectors: []string{finding.DetectorID},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	typedFinding, err := findingContract(finding)
	if err != nil {
		t.Fatal(err)
	}
	completed := time.Now().UTC()
	closure := &contracts.IncidentClosure{
		Request: &contracts.IncidentRequest{
			IncidentId: loop.Request.IncidentID, SourceCommit: loop.Request.SourceCommit,
			Findings:        []*contracts.Finding{typedFinding},
			DetectorHistory: []*contracts.DetectorEvaluation{{DetectorId: finding.DetectorID, Status: "firing", EvaluatedAt: timestamppb.New(completed.Add(-time.Second))}},
		},
		Result: message, DetectedAt: timestamppb.New(completed.Add(-2 * time.Second)),
		DispatchedAt: timestamppb.New(completed.Add(-time.Second)), ResponderCompletedAt: timestamppb.New(completed),
		VerifiedAt: timestamppb.New(completed.Add(time.Second)), HealthClearedAt: timestamppb.New(completed),
		FinalDetectorStates: []*contracts.DetectorEvaluation{{DetectorId: finding.DetectorID, Status: "clear", EvaluatedAt: timestamppb.New(completed)}},
	}
	derived, err := outcome_derivation.Derive(context.Background(), closure, "", "scripted", "none")
	if err != nil {
		t.Fatal(err)
	}
	if derived.Record.GetClassification() != "success" {
		t.Fatalf("fixture did not independently derive success: %+v", derived.Record)
	}
	return derived.Record
}
