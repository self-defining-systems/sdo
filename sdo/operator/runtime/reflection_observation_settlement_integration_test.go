//go:build integration

package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// The real executor and stage produce a confirmed deletion followed by a cache
// observation failure. The outcome producer then carries independently cleared
// health into the reflection consumer, through the shared recovery owner before outcome/learning projection.
func TestConfirmedLifecycleStageFeedsReflectionSettlement(t *testing.T) {
	for _, kind := range []string{"ResourceQuota", "Deployment"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			if err := os.MkdirAll(filepath.Join(root, ".sdo/playbooks/uncached-policy"), 0o755); err != nil {
				t.Fatal(err)
			}
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: kind, Namespace: "app", Name: "synthetic-policy"}
			if kind == "Deployment" {
				ref.APIVersion = "apps/v1"
			}
			object := planObject(ref, map[string]any{"hard": map[string]any{"memory": "1Gi"}})
			object.SetUID("synthetic-policy")
			object.SetResourceVersion("1")
			client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}}
			source := lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{}}
			provider := uncachedPolicyProvider{client: client, source: source}
			detector, p := uncachedPolicyDetector{ref: ref}, settle3DeletionProgram{kind: kind}
			snapshot, err := provider.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			findings, err := detector.Detect(ctx, snapshot)
			if err != nil || len(findings) != 1 {
				t.Fatalf("initial finding: %v %v", findings, err)
			}
			catalog, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
			if err != nil {
				t.Fatal(err)
			}
			request := goldenRequest(t)
			request.Namespace, request.Findings, request.RepairPolicy, request.RepositoryWorktree = "app", findings, "recorded-actions", root
			started := time.Now()
			request.ResponseDeadline = started.Add(time.Minute)
			executor := PlanExecutor{Client: client, Source: source, Observer: lifecycleObservationFailure{}, Rollouts: settle3DeletionRollout{client: client}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{detector}, Provider: provider}}
			loop := NewResponderLoop(request, catalog, provider, []sdk.Detector{detector}, executor, 5)
			loop.ReadView = program.ReadFrom
			state := &IncidentState{Request: request, Findings: findings}
			report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(ctx, state)
			if err != nil || len(report.GetMitigations()) != 1 || report.Mitigations[0].GetResult() != "rollout_failed" {
				t.Fatalf("stage did not retain observation failure: %v %v", report, err)
			}
			if len(report.Mitigations[0].GetClearedFindingFingerprints()) == 0 || !report.Mitigations[0].GetApplyReceipt().GetObjectEffects()[0].GetConfirmed() {
				t.Fatalf("missing real stage proof: %v", report)
			}
			snapshot, err = provider.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			findings, err = detector.Detect(ctx, snapshot)
			if err != nil || len(findings) != 0 {
				t.Fatalf("independent final detector did not clear: %v %v", findings, err)
			}
			request.PriorStages = []*contracts.StageReport{report}
			finished := time.Now()
			closure := IncidentClosure{
				Request: request, Result: &IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentID: request.IncidentID, Status: IncidentCompleted, PlanApplications: loop.PlanApplications()},
				DetectedAt: started, DispatchedAt: started, ResponderCompletedAt: finished, VerifiedAt: finished,
				FinalDetectorStates: []DetectorEvaluation{{DetectorID: detector.Spec().ID, Status: DetectorEvaluationClear, EvaluatedAt: finished}},
			}
			*closure.Result, closure.Request.PriorStages = loop.SettleRecoveryApplications(ctx, *closure.Result, closure.Request.PriorStages)
			if got := closure.Result.PlanApplications[0].Result.FinalVerificationBasis; got != "recovery-settled" {
				t.Fatalf("confirmed lifecycle recovery lost application settlement: %q", got)
			}
			derived, err := (ProtoOutcomeDeriver{}).Derive(ctx, closure, "", "test", "test")
			if err != nil {
				t.Fatal(err)
			}
			diagnostics := validate.ReflectionRepairDiagnostics(root, derived.Record, &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "covered by confirmed lifecycle repair"}, nil)
			if len(diagnostics) != 0 {
				t.Fatalf("real producer proof was rejected: %v\noutcome: %v", diagnostics, derived.Record)
			}
		})
	}
}
