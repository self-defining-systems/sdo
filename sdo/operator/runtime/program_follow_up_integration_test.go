//go:build integration

package runtime

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func rollLoop(t *testing.T, p rollProgram) (*ResponderLoop, *rollCluster, string) {
	t.Helper()
	return rollLoopFor(t, p)
}

func rollLoopFor(t *testing.T, p interface {
	program.Diagnoser
	program.Mitigator
}) (*ResponderLoop, *rollCluster, string) {
	t.Helper()
	deployment := planObject(rollWeb, map[string]any{"replicas": int64(2)})
	deployment.SetUID("web-uid")
	deployment.SetAnnotations(map[string]string{rollRevision: "1"})
	set := planObject(rollOldRS, map[string]any{"replicas": int64(1)})
	set.SetUID("rs-old")
	set.SetAnnotations(map[string]string{rollRevision: "1"})
	set.Object["metadata"].(map[string]any)["ownerReferences"] = []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "web-uid", "controller": true}}
	set.Object["status"] = map[string]any{"readyReplicas": int64(1)}
	cluster := &rollCluster{planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{rollWeb: deployment, rollOldRS: set}}}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	finding := sdk.Finding{Status: sdk.FindingActive, PrimaryResource: rollWeb, ParameterBindings: map[string]sdk.ObjectRef{"deployment": rollWeb}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "roll", Namespace: "app", SourceCommit: "source", RepairPolicy: "recorded-actions", ResponseDeadline: time.Now().Add(10 * time.Minute), Findings: []sdk.Finding{finding}}, registry, cluster, nil, PlanExecutor{Client: cluster, Rollouts: rollRollouts{cluster}, Verifier: planFakeVerifier{true}}, 5)
	loop.ReadView = func(view sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: view.(sdktest.Snapshot)}
	}
	loop.MenuConfig = &ResponderMenuConfig{}
	loop.ProgramFollowUpSettle = time.Millisecond
	menu := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	for _, option := range menu.Menu.GetOptions() {
		if option.GetProgramId() == p.Spec().ID {
			return loop, cluster, option.GetOptionId()
		}
	}
	t.Fatalf("program option missing: %+v", menu)
	return nil, nil, ""
}

func TestProgramFollowUpRetiresOldReplicaSetAfterDeadlockedRollout(t *testing.T) {
	loop, cluster, option := rollLoop(t, rollProgram{cleanup: true})
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	if response.Report.GetStatus() != contracts.MitigationReport_STATUS_SUCCESS || response.RolloutPending != nil {
		t.Fatalf("program did not finish its repair after the rollout deadlocked: %+v %+v", response.Report, response.MenuExecution)
	}
	if cluster.blocked() {
		t.Fatal("old-template ReplicaSet was not retired")
	}
	retired := false
	for _, receipt := range loop.MenuRepairActions() {
		retired = retired || receipt.Target == "ReplicaSet/web-old"
	}
	if !retired || loop.LoopsUsed() != 1 || len(loop.pendingWrites) != 0 {
		t.Fatalf("follow-up lost its receipt, consumed a batch or kept a lock: receipts=%+v loops=%d pending=%d", loop.MenuRepairActions(), loop.LoopsUsed(), len(loop.pendingWrites))
	}
}

func TestStalledProgramApplicationReleasesLockToResponder(t *testing.T) {
	loop, cluster, option := rollLoop(t, rollProgram{})
	loop.PendingStallTimeout = time.Millisecond
	first := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	if first.RolloutPending == nil {
		t.Fatalf("setup: expected a deadlocked rollout: %+v", first.Report)
	}
	time.Sleep(5 * time.Millisecond)
	scale := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "ScaleWorkload", RoundLimit: 1}, Parameters: map[string]string{"ref": "Deployment/app/web", "replicas": "1"}})
	if scale.Report.GetReasonCode() == "pending_write_conflict" {
		t.Fatalf("responder waited on a stalled program lock: %s", scale.Error)
	}
	report := loop.Handle(context.Background(), ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{AttemptId: first.Report.GetAttemptId()}})
	if report.Report.GetReasonCode() != "rollout_stalled" || report.Report.GetStatus() == contracts.MitigationReport_STATUS_SUCCESS {
		t.Fatalf("stalled application lacks its typed reason: %+v", report.Report)
	}
	// The responder wrote the released target, so a later completion is not the program's.
	rollRetireOld(cluster)
	late := loop.Handle(context.Background(), ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{AttemptId: first.Report.GetAttemptId()}})
	if late.Report.GetStatus() == contracts.MitigationReport_STATUS_SUCCESS {
		t.Fatalf("program received credit for a rollout the responder's later write may have finished: %+v", late.Report)
	}
}

func TestSlowHealthyRolloutAfterStallReleaseKeepsProgramSuccess(t *testing.T) {
	loop, cluster, option := rollLoop(t, rollProgram{})
	loop.PendingStallTimeout = time.Millisecond
	first := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	if first.RolloutPending == nil {
		t.Fatalf("setup: expected a slow rollout: %+v", first.Report)
	}
	time.Sleep(5 * time.Millisecond)
	stalled := loop.Handle(context.Background(), ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{AttemptId: first.Report.GetAttemptId()}})
	if stalled.Report.GetReasonCode() != "rollout_stalled" {
		t.Fatalf("setup: expected the lock release: %+v", stalled.Report)
	}
	_, release, ok := acquireClosureGuard(loop)
	release()
	if !ok {
		t.Fatal("released application still blocks the responder's closure check")
	}
	rollRetireOld(cluster) // the old pod finally terminates; nobody else wrote the target
	settled := loop.Handle(context.Background(), ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{AttemptId: first.Report.GetAttemptId()}})
	if settled.Report.GetStatus() != contracts.MitigationReport_STATUS_SUCCESS || settled.Report.GetReasonCode() != "" {
		t.Fatalf("slow healthy rollout lost the program's success: %+v", settled.Report)
	}
	for _, application := range loop.applicationsLog {
		if application.ID == first.ApplicationID && (application.Error != "" || application.Pending || !application.Result.VerificationCompleted || !application.Result.VerificationPassed) {
			t.Fatalf("learning receipt still records a stall: %+v", application)
		}
	}
	if len(loop.pendingWrites) != 0 {
		t.Fatal("settled reservation remained")
	}
}

// A dup-PVC repeat: after the release the responder retires the old ReplicaSet,
// a different object than the program's Deployment. Its write finished the
// rollout, so the program must not earn credit for it.
func TestResponderChildWriteAfterStallReleaseDeniesProgramCredit(t *testing.T) {
	loop, cluster, option := rollLoop(t, rollProgram{})
	loop.PendingStallTimeout = time.Millisecond
	first := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	if first.RolloutPending == nil {
		t.Fatalf("setup: expected a deadlocked rollout: %+v", first.Report)
	}
	time.Sleep(5 * time.Millisecond)
	if stalled := loop.Handle(context.Background(), ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{AttemptId: first.Report.GetAttemptId()}}); stalled.Report.GetReasonCode() != "rollout_stalled" {
		t.Fatalf("setup: expected the lock release: %+v", stalled.Report)
	}
	scale := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "ScaleWorkload", RoundLimit: 1}, Parameters: map[string]string{"ref": "ReplicaSet/app/web-old", "replicas": "0"}})
	if scale.Error != "" || cluster.blocked() {
		t.Fatalf("setup: the responder's ReplicaSet scale did not apply: %s %+v", scale.Error, scale.Report)
	}
	late := loop.Handle(context.Background(), ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{AttemptId: first.Report.GetAttemptId()}})
	if late.Report.GetStatus() == contracts.MitigationReport_STATUS_SUCCESS {
		t.Fatalf("program received credit for a rollout the responder's child write finished: %+v", late.Report)
	}
	for _, application := range loop.applicationsLog {
		if application.ID == first.ApplicationID && application.Result.VerificationPassed {
			t.Fatalf("learning receipt credits the program: %+v", application)
		}
	}
}

// A first plan that retires the ReplicaSet still serving the Deployment would
// cause an outage; the runtime refuses it before any write.
func TestProgramScaleOfCurrentReplicaSetIsRefused(t *testing.T) {
	loop, cluster, option := rollLoop(t, rollProgram{oneShot: true})
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	if response.Report.GetReasonCode() != "replicaset_scale_refused" {
		t.Fatalf("scaling the current revision must be a typed refusal: %+v %+v", response.Report, response.MenuExecution)
	}
	if !cluster.blocked() || cluster.objects[rollNewPod] != nil {
		t.Fatal("a refused plan wrote to the cluster")
	}
}

func TestProgramFollowUpStopsOnRepeatedPlanCycle(t *testing.T) {
	loop, _, option := rollLoop(t, rollProgram{flip: true})
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	applied := 0
	for _, round := range response.MenuExecution {
		if round.ApplicationID != "" {
			applied++
		}
	}
	if applied != 2 {
		t.Fatalf("an A/B/A plan cycle must stop at its first repeat, applied %d rounds: %+v", applied, response.MenuExecution)
	}
}

func TestProgramFollowUpFailureIsSignaledAndReported(t *testing.T) {
	loop, _, option := rollLoop(t, rollProgram{fail: true})
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	signaled := false
	for _, signal := range loop.failureSignals {
		signaled = signaled || signal == "mitigator_failure:rollout-cleanup"
	}
	reported := false
	for _, round := range response.MenuExecution {
		reported = reported || strings.Contains(round.Error, "cannot read the rollout")
	}
	if !signaled || !reported {
		t.Fatalf("follow-up mitigator failure lost: signals=%v rounds=%+v", loop.failureSignals, response.MenuExecution)
	}
}

// A follow-up writes only through the ordinary reservation check: another
// application's held write on the same object refuses it.
func TestProgramFollowUpRespectsOtherHeldReservations(t *testing.T) {
	loop, cluster, option := rollLoop(t, rollProgram{cleanup: true})
	loop.pendingWrites = map[string]pendingMitigation{"other": {Refs: []sdk.ObjectRef{rollOldRS}, AttemptID: "other", ReservedAt: time.Now().UTC()}}
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	if !cluster.blocked() {
		t.Fatal("follow-up wrote an object another application holds")
	}
	if !strings.Contains(fmt.Sprint(response.Report, response.MenuExecution), "pending_write_conflict") {
		t.Fatalf("follow-up conflict lacks its typed reason: %+v %+v", response.Report, response.MenuExecution)
	}
}

// A pending application shown ineffective (its own diagnosis still matches
// after its writes, its targeted finding still active) fails explicitly and
// releases the object, so an independent repair runs without a stall timeout.
func TestIneffectivePendingApplicationReleasesObjectToIndependentRepair(t *testing.T) {
	loop, cluster, option := rollLoopFor(t, wrongPlanProgram{})
	loop.PendingStallTimeout = time.Hour
	first := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	if first.RolloutPending == nil {
		t.Fatalf("setup: expected a blocked rollout: %+v %s", first.Report, first.Error)
	}
	scale := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "ScaleWorkload", RoundLimit: 1}, Parameters: map[string]string{"ref": "Deployment/app/web", "replicas": "2"}})
	if scale.Report.GetReasonCode() == "pending_write_conflict" || scale.Error != "" {
		t.Fatalf("an ineffective pending plan blocked the independent repair: %s %+v", scale.Error, scale.Report)
	}
	report := loop.Handle(context.Background(), ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{AttemptId: first.Report.GetAttemptId()}})
	if report.Report.GetReasonCode() != "diagnosis_persists" || report.Report.GetStatus() == contracts.MitigationReport_STATUS_SUCCESS {
		t.Fatalf("ineffective application lacks its explicit failure: %+v", report.Report)
	}
	rollRetireOld(cluster)
	late := loop.Handle(context.Background(), ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{AttemptId: first.Report.GetAttemptId()}})
	if late.Report.GetStatus() == contracts.MitigationReport_STATUS_SUCCESS || late.Report.GetReasonCode() != "diagnosis_persists" {
		t.Fatalf("failed application earned late credit or lost its reason: %+v", late.Report)
	}
}

// An application whose diagnosis no longer matches is still converging: it keeps
// its object until its rollout settles or stalls.
func TestEffectivePendingApplicationKeepsItsObject(t *testing.T) {
	loop, _, option := rollLoop(t, rollProgram{})
	loop.PendingStallTimeout = time.Hour
	first := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	if first.RolloutPending == nil {
		t.Fatalf("setup: expected a blocked rollout: %+v %s", first.Report, first.Error)
	}
	loop.pendingWrites[first.ApplicationID] = func(p pendingMitigation) pendingMitigation {
		p.FindingFingerprints = []string{"cleared"}
		return p
	}(loop.pendingWrites[first.ApplicationID])
	scale := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "ScaleWorkload", RoundLimit: 1}, Parameters: map[string]string{"ref": "Deployment/app/web", "replicas": "2"}})
	if scale.Report.GetReasonCode() != "pending_write_conflict" {
		t.Fatalf("a pending application whose targeted findings cleared lost its object: %+v", scale.Report)
	}
}

