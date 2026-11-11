package runtime

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func statusTestApplication(result PlanApplicationResult) RecordedPlanApplication {
	started := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return RecordedPlanApplication{ID: "app-1", ProgramIDs: []string{"action:RestartRollout"}, Result: result, StartedAt: started, CompletedAt: started.Add(time.Second)}
}

func statusTestRef() sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "synthetic", Name: "web"}
}

func appliedRestart(verificationCompleted, verificationPassed bool) PlanApplicationResult {
	action := program.RestartRollout{Ref: statusTestRef()}
	return PlanApplicationResult{
		Plan:                  program.Plan{Actions: []program.Action{action}},
		Applied:               true,
		ActionsApplied:        []program.Action{action},
		ActionReports:         []PlanActionReport{{Status: "applied"}},
		ObjectEffects:         []PlanObjectEffect{{Operation: "patch", Ref: statusTestRef(), State: "applied", Confirmed: true}},
		VerificationCompleted: verificationCompleted,
		VerificationPassed:    verificationPassed,
	}
}

func onlyProgramReceipt(t *testing.T, receipts []RepairActionReceipt) RepairActionReceipt {
	t.Helper()
	if len(receipts) != 1 {
		t.Fatalf("receipts = %+v, want exactly one", receipts)
	}
	if err := wire.Validate(repairActionContract(receipts[0])); err != nil {
		t.Fatalf("receipt %+v violates the contract: %v", receipts[0], err)
	}
	return receipts[0]
}

// A responder-driven menu action is never verified by itself; the responder
// verifies the incident as a whole. Its applied write is not a failure.
func TestProgramReceiptAppliedWithoutOwnVerificationIsAppliedNotFailed(t *testing.T) {
	receipt := onlyProgramReceipt(t, ProgramRepairReceipts([]RecordedPlanApplication{statusTestApplication(appliedRestart(false, false))}, nil))
	if receipt.Status != contracts.RepairActionStatus_REPAIR_ACTION_STATUS_APPLIED || receipt.FailureReason != "" {
		t.Fatalf("status = %v reason = %q, want APPLIED without a reason", receipt.Status, receipt.FailureReason)
	}
}

func TestProgramReceiptAppliedAndVerifiedIsVerified(t *testing.T) {
	receipt := onlyProgramReceipt(t, ProgramRepairReceipts([]RecordedPlanApplication{statusTestApplication(appliedRestart(true, true))}, nil))
	if receipt.Status != contracts.RepairActionStatus_REPAIR_ACTION_STATUS_VERIFIED || !receipt.Success {
		t.Fatalf("status = %v success = %v, want VERIFIED and successful", receipt.Status, receipt.Success)
	}
}

func TestProgramReceiptActionFailureStaysFailed(t *testing.T) {
	result := appliedRestart(false, false)
	result.ActionReports = []PlanActionReport{{Status: "failed", FailureReason: "execution_failed"}}
	result.ObjectEffects = nil
	receipt := onlyProgramReceipt(t, ProgramRepairReceipts([]RecordedPlanApplication{statusTestApplication(result)}, nil))
	if receipt.Status != contracts.RepairActionStatus_REPAIR_ACTION_STATUS_FAILED || receipt.FailureReason != "execution_failed" {
		t.Fatalf("status = %v reason = %q, want FAILED with the action's reason", receipt.Status, receipt.FailureReason)
	}
}

func TestProgramReceiptRolledBackIsFailed(t *testing.T) {
	application := statusTestApplication(appliedRestart(true, true))
	application.RolledBack = true
	receipt := onlyProgramReceipt(t, ProgramRepairReceipts([]RecordedPlanApplication{application}, nil))
	if receipt.Status != contracts.RepairActionStatus_REPAIR_ACTION_STATUS_FAILED || receipt.Success {
		t.Fatalf("status = %v success = %v, want FAILED and unsuccessful", receipt.Status, receipt.Success)
	}
}

// Prior-attempt history shown to the next responder reports an applied but
// unverified action as progress and only a failed action as an error.
func TestResponseProgressReportsAppliedActionsAsProgressNotErrors(t *testing.T) {
	ref := statusTestRef()
	finding := sdk.Finding{DetectorID: "synthetic-detector", Fingerprint: "fp", PrimaryResource: ref}
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	applied := repairActionContract(onlyProgramReceipt(t, ProgramRepairReceipts([]RecordedPlanApplication{statusTestApplication(appliedRestart(false, false))}, nil)))
	failed := repairActionContract(RepairActionReceipt{ActionID: "failed-1", Kind: "program-plan", Target: "Deployment/web", Summary: "program plan action failed", Details: "synthetic", StartedAt: at, CompletedAt: at, FailureReason: "execution_failed", Status: contracts.RepairActionStatus_REPAIR_ACTION_STATUS_FAILED})
	record := &contracts.OutcomeRecord{
		IncidentId: "incident-1", Classification: "failed",
		Findings:      []*contracts.Finding{{DetectorId: finding.DetectorID, Fingerprint: FindingFingerprint(finding), PrimaryResource: &contracts.ObjectRef{ApiVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}}},
		RepairActions: []*contracts.RepairActionReceipt{applied, failed},
		Timestamps:    &contracts.OutcomeTimestamps{CompletedAt: timestamppb.New(at)},
	}
	progress := responseProgress([]*contracts.OutcomeRecord{record}, []sdk.Finding{finding}, time.Time{})
	if len(progress) != 1 {
		t.Fatalf("progress = %v, want one episode", progress)
	}
	if got := progress[0].GetSuccessfulActions(); len(got) != 1 || got[0].GetActionId() != applied.GetActionId() {
		t.Fatalf("successful actions = %v, want only the applied action", got)
	}
	if text := progress[0].GetError(); !strings.Contains(text, "execution_failed") || strings.Contains(text, applied.GetSummary()) {
		t.Fatalf("error = %q, want only the failed action", text)
	}
}
