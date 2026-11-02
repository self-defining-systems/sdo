package runtime

import (
	"fmt"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"strings"
	"testing"
	"time"
)

func TestFailedResponseProgressCarriesSuccessfulReceiptsAndBlockers(t *testing.T) {
	f := stateFinding("health")
	f.Fingerprint = "same-open-problem"
	record := &contracts.OutcomeRecord{IncidentId: "failed-episode", Classification: "failed", Findings: []*contracts.Finding{{DetectorId: f.DetectorID, Fingerprint: f.Fingerprint, PrimaryResource: planRefProto(f.PrimaryResource)}}, RepairActions: []*contracts.RepairActionReceipt{{ActionId: "source-fixed", Success: true, Summary: "source reconciled"}, {ActionId: "failed-write", Success: false, FailureReason: "command_failed", Details: "webhook validate.example.test denied Pods"}}, PlanApplications: []*contracts.PlanApplication{{Applied: true, RolloutPending: &contracts.RolloutPending{Reason: `admission_blocked: webhook validate.example.test denied Pods`}}}}
	progress := responseProgress([]*contracts.OutcomeRecord{record}, []sdk.Finding{f}, time.Time{})
	if len(progress) != 1 || len(progress[0].GetSuccessfulActions()) != 1 || progress[0].SuccessfulActions[0].ActionId != "source-fixed" || len(progress[0].GetBlockers()) != 1 {
		t.Fatalf("failed episode lost progress: %+v", progress)
	}
	if !strings.Contains(progress[0].GetError(), "webhook validate.example.test denied Pods") {
		t.Fatalf("failed action lost actionable error details: %v", progress[0])
	}
	unrelated := f
	unrelated.Fingerprint = "other-problem"
	if got := responseProgress([]*contracts.OutcomeRecord{record}, []sdk.Finding{unrelated}, time.Time{}); len(got) != 0 {
		t.Fatal("unrelated failure recalled")
	}
	request := IncidentRequest{PriorResponseProgress: progress}
	wire, err := incidentRequestContract(request)
	if err != nil {
		t.Fatal(err)
	}
	restored := incidentRequestState(wire)
	if len(restored.PriorResponseProgress) != 1 {
		t.Fatal("next request lost progress")
	}
}

func TestFailedResponseProgressEndsAtHealthyClosureAndRequiresSameResource(t *testing.T) {
	f := stateFinding("health")
	f.Fingerprint = "open"
	failed := &contracts.OutcomeRecord{IncidentId: "old", Classification: "failed", Findings: []*contracts.Finding{{DetectorId: f.DetectorID, Fingerprint: f.Fingerprint, PrimaryResource: planRefProto(f.PrimaryResource)}}, RepairActions: []*contracts.RepairActionReceipt{{ActionId: "done", Success: true}}}
	for _, classification := range []string{"success", "cleared_without_action"} {
		healthy := &contracts.OutcomeRecord{IncidentId: "healthy", Classification: classification, Timestamps: &contracts.OutcomeTimestamps{CompletedAt: contractTimestamp(time.Unix(2, 0))}}
		if got := responseProgress([]*contracts.OutcomeRecord{failed, healthy}, []sdk.Finding{f}, time.Time{}); len(got) != 0 {
			t.Fatalf("%s failed to end prior open work: %v", classification, got)
		}
	}
	f.PrimaryResource.Name = "different"
	if got := responseProgress([]*contracts.OutcomeRecord{failed}, []sdk.Finding{f}, time.Time{}); len(got) != 0 {
		t.Fatalf("same fingerprint on different resource recalled old work: %v", got)
	}
}

func TestFailedResponseProgressEndsWhenFailedWorkerWasIndependentlyHealthy(t *testing.T) {
	f := stateFinding("health")
	f.Fingerprint = "same-fault-returned"
	// Outcome classification follows failed worker status even when the
	// controller independently observed healthy closure. That still ends work.
	record := &contracts.OutcomeRecord{IncidentId: "failed-but-cleared", Classification: "failed", Timestamps: &contracts.OutcomeTimestamps{VerifiedAt: contractTimestamp(time.Unix(2, 0)), CompletedAt: contractTimestamp(time.Unix(2, 0))}, Findings: []*contracts.Finding{{DetectorId: f.DetectorID, Fingerprint: f.Fingerprint, PrimaryResource: planRefProto(f.PrimaryResource)}}, RepairActions: []*contracts.RepairActionReceipt{{ActionId: "old-repair", Success: true}}}
	if got := responseProgress([]*contracts.OutcomeRecord{record}, []sdk.Finding{f}, time.Time{}); len(got) != 0 {
		t.Fatalf("closed healthy problem reopened as already-done work: %v", got)
	}
}

func TestFailedResponseProgressBudgetsBoundEveryNestedInventory(t *testing.T) {
	for _, budget := range []int{4 << 10, maxResponseProgressBytes} {
		for _, text := range []string{strings.Repeat("<>&", 1000), strings.Repeat("界", 1000), strings.Repeat("\\\"\n", 1000)} {
			item := &contracts.ResponseProgress{IncidentId: "newest", Error: text}
			for i := 0; i < 20; i++ {
				item.SuccessfulActions = append(item.SuccessfulActions, &contracts.RepairActionReceipt{ActionId: fmt.Sprint(i), Summary: text, Details: text})
				item.Blockers = append(item.Blockers, &contracts.RolloutPending{Reason: text, AdmissionWebhooks: []string{text, text, text, text, text}})
			}
			got := boundResponseProgress([]*contracts.ResponseProgress{item}, budget)
			if progressWireBytes(got) > budget || len(got) != 1 || !got[0].GetTruncated() {
				t.Fatalf("budget %d violated: %d", budget, progressWireBytes(got))
			}
			if len(got[0].SuccessfulActions) == 0 || got[0].SuccessfulActions[len(got[0].SuccessfulActions)-1].ActionId != "19" || len(got[0].Blockers) == 0 {
				t.Fatalf("newest actionable evidence lost at %d: %v", budget, got)
			}
			if len(item.SuccessfulActions) != 20 || item.Error != text {
				t.Fatal("durable source mutated")
			}
		}
	}
}
