package runtime

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"testing"
	"time"
)

func TestFailedResponseProgressCarriesSuccessfulReceiptsAndBlockers(t *testing.T) {
	f := stateFinding("health")
	f.Fingerprint = "same-open-problem"
	record := &contracts.OutcomeRecord{IncidentId: "failed-episode", Classification: "failed", Findings: []*contracts.Finding{{DetectorId: f.DetectorID, Fingerprint: f.Fingerprint, PrimaryResource: planRefProto(f.PrimaryResource)}}, RepairActions: []*contracts.RepairActionReceipt{{ActionId: "source-fixed", Success: true, Summary: "source reconciled"}, {ActionId: "failed-write", Success: false}}, PlanApplications: []*contracts.PlanApplication{{Applied: true, RolloutPending: &contracts.RolloutPending{Reason: `admission_blocked: webhook validate.example.test denied Pods`}}}}
	progress := responseProgress([]*contracts.OutcomeRecord{record}, []sdk.Finding{f}, time.Time{})
	if len(progress) != 1 || len(progress[0].GetSuccessfulActions()) != 1 || progress[0].SuccessfulActions[0].ActionId != "source-fixed" || len(progress[0].GetBlockers()) != 1 {
		t.Fatalf("failed episode lost progress: %+v", progress)
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

// Exercise the durable producer -> next controller request -> wire consumer,
// rather than placing progress directly into an already-built request.
func TestFailedEpisodeNextControllerRequestRetainsDurableProgress(t *testing.T) {
	repository := t.TempDir()
	recordOutcomes(t, repository, `{"incident_id":"failed","classification":"failed","findings":[{"detector_id":"health","rule_id":"missing","status":"active","severity":"critical","summary":"missing","evidence":"missing","primary_resource":{"kind":"ConfigMap","namespace":"app","name":"cfg"},"fingerprint":"missing:cfg"}],"repair_actions":[{"action_id":"source-fixed","kind":"source","target":"app/cfg","summary":"source reconciled","details":"successful readback","started_at":"2026-01-01T00:00:00Z","completed_at":"2026-01-01T00:00:01Z","success":true,"reversible":false}],"program_failure_signals":["admission_blocked: validate.example.test denies Pods"]}`)
	c := &Controller{config: testControllerConfig()}
	c.config.RepositoryWorktree = repository
	request := c.incidentRequest(time.Now(), []sdk.Finding{{DetectorID: "health", Fingerprint: "missing:cfg", PrimaryResource: sdk.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "cfg"}}})
	wire, err := incidentRequestContract(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire.GetPriorResponseProgress()) != 1 || wire.PriorResponseProgress[0].SuccessfulActions[0].Summary != "source reconciled" || wire.PriorResponseProgress[0].Error != "admission_blocked: validate.example.test denies Pods" {
		t.Fatalf("failed episode progress absent from next request: %v", wire.GetPriorResponseProgress())
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
