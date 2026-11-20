package runtime

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

const residualGoodImage = "api:v1"

// residualWorld is one Deployment whose live image drifted from accepted
// source. Its live object also differs from source at a path the fault never
// touched and a reconcile cannot move (as a pack's live deployment differs from
// its source), so every readback reports that path as a residual.
type residualWorld struct{ *sameWriteWorld }

func (w residualWorld) AcceptedManifest(ctx context.Context, _ string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	live, err := TypedPlanClient{Client: w.client}.Get(ctx, ref)
	if err != nil {
		return nil, err
	}
	var deployment appsv1.Deployment
	if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(live.Object, &deployment); err != nil {
		return nil, err
	}
	deployment.Spec.Template.Spec.Containers[0].Image = residualGoodImage
	object, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(&deployment)
	return &unstructured.Unstructured{Object: object}, err
}

// SourceRolloutResiduals reports the pre-existing difference.
func (residualWorld) SourceRolloutResiduals(context.Context, *unstructured.Unstructured) ([]string, error) {
	return []string{"spec.template.spec.containers[0].livenessProbe.initialDelaySeconds"}, nil
}

func (w residualWorld) image(t *testing.T) string {
	t.Helper()
	deployment, err := w.client.AppsV1().Deployments("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return deployment.Spec.Template.Spec.Containers[0].Image
}

type residualDetector struct{ world residualWorld }

func (d residualDetector) Spec() sdk.DetectorSpec {
	spec := sameWriteDetector{}.Spec()
	spec.Playbooks = []string{"source-drift"}
	return spec
}

func (d residualDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	var findings []sdk.Finding
	for _, deployment := range view.Deployments() {
		if deployment.Spec.Template.Spec.Containers[0].Image != residualGoodImage {
			findings = append(findings, sdk.Finding{DetectorID: "workload-health", RuleID: "image-drift", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment), Summary: "image differs from source", Evidence: "synthetic drift", Playbooks: []string{"source-drift"}})
		}
	}
	return findings, nil
}

// sourceDriftProgram matches a Deployment whose image differs from source and
// restores it from source.
type sourceDriftProgram struct{}

func (sourceDriftProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "source-drift", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}}
}

func (sourceDriftProgram) Diagnose(_ context.Context, view program.ReadView, b program.Bindings) (program.Verdict, error) {
	for _, deployment := range view.Deployments() {
		if deployment.Name == b.Objects["deployment"].Name && deployment.Spec.Template.Spec.Containers[0].Image != residualGoodImage {
			return program.Verdict{Result: program.Match}, nil
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func (sourceDriftProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: b.Objects["deployment"]}}}, nil
}

// A source restore is judged on the program's own post-apply diagnosis and the
// opening alerts. A path that differs from source before and after the fault
// is recorded as evidence and is neither a failed apply nor a failure against
// the playbook.
func TestSourceRestoreWithPreExistingResidualIsNotAFailedApply(t *testing.T) {
	ctx := context.Background()
	sameWorld, loop, _ := sameWriteLoop(t)
	world := residualWorld{sameWorld}
	deployment, err := world.client.AppsV1().Deployments("app").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	deployment.Spec.Template.Spec.Containers[0].Image = "api:bad"
	if _, err := world.client.AppsV1().Deployments("app").Update(ctx, deployment, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	detector := residualDetector{world}
	registry, err := program.NewRegistry([]program.Diagnoser{sourceDriftProgram{}}, []program.Mitigator{sourceDriftProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := world.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := detector.Detect(ctx, snapshot)
	if err != nil || len(findings) != 1 {
		t.Fatalf("findings = %v, %v", findings, err)
	}
	request := IncidentRequest{IncidentID: "synthetic-residual", Namespace: "app", SourceCommit: "synthetic-accepted", Findings: findings, RepairPolicy: "recorded-actions"}
	executor := PlanExecutor{Client: TypedPlanClient{Client: world.client}, Source: world, Rollouts: world, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{detector}, Provider: world}}
	loop = NewResponderLoop(request, registry, world, []sdk.Detector{detector}, executor, 5)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView { return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)} }
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	state := &IncidentState{Request: request, Findings: findings}

	instant := newInstantRecoveryClock()
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true,
		recoveryNow: instant.now, recoveryWait: instant.wait, WaitBound: time.Minute}
	report, err := stage.Run(ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	if world.image(t) != residualGoodImage {
		t.Fatalf("the source restore did not land: image %q", world.image(t))
	}
	if len(report.Mitigations) != 1 {
		t.Fatalf("mitigations = %v", report.Mitigations)
	}
	attempt := report.Mitigations[0]
	receipt := attempt.GetApplyReceipt()
	if attempt.Result != "applied_cleared" || !receipt.GetVerificationPassed() || receipt.GetError() != "" ||
		receipt.GetSettlementRejectionReason() != contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_UNSPECIFIED {
		t.Fatalf("a working apply was judged by a pre-existing residual: result=%s rejection=%s error=%q", attempt.Result, receipt.GetSettlementRejectionReason(), receipt.GetError())
	}
	if exit := report.GetRecoveryWait().GetExitReason(); exit != contracts.RecoveryWait_EXIT_REASON_CLEARED {
		t.Fatalf("stage exit = %s, want CLEARED", exit)
	}
	var residual bool
	for _, action := range receipt.GetActionReports() {
		residual = residual || len(action.GetSourceReconcile().GetResidualPaths()) > 0
	}
	if !residual {
		t.Fatal("the residual stays recorded as evidence")
	}
	trust := ProgramTrustStats([]*contracts.OutcomeRecord{{Classification: "cleared_without_action", SourceCommit: "synthetic-accepted", PlanApplications: []*contracts.PlanApplication{receipt}, PriorStages: []*contracts.StageReport{report}}})
	if got := trust["source-drift"].Failures; got != 0 {
		t.Fatalf("the playbook was charged %d failures for a residual that predates the fault", got)
	}
}
