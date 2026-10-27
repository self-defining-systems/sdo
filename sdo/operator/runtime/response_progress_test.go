package runtime

import (
	"encoding/json"
	"fmt"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/worker_registry"
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

func TestFailedResponseProgressProducerFitsDispatchAtLimits(t *testing.T) {
	for _, episodes := range []int{8, 9, 80} {
		t.Run(fmt.Sprint(episodes), func(t *testing.T) {
			repository := t.TempDir()
			var lines []string
			for i := 0; i < episodes; i++ {
				record := map[string]any{"incident_id": fmt.Sprintf("failed-%03d", i), "classification": "failed", "timestamps": map[string]any{"completed_at": time.Unix(int64(i+1), 0).UTC().Format(time.RFC3339)}, "findings": []any{map[string]any{"detector_id": "health", "rule_id": "missing", "status": "active", "severity": "critical", "summary": "missing", "evidence": "missing", "primary_resource": map[string]any{"kind": "ConfigMap", "namespace": "app", "name": "cfg"}, "fingerprint": "missing:cfg"}}, "repair_actions": []any{map[string]any{"action_id": fmt.Sprintf("fixed-%03d", i), "kind": "source", "target": "app/cfg", "summary": "source reconciled", "details": strings.Repeat("verbose readback ", 4096), "started_at": "2026-01-01T00:00:00Z", "completed_at": "2026-01-01T00:00:01Z", "success": true}}, "plan_applications": []any{map[string]any{"plan": map[string]any{}, "applied": true, "rollout_pending": map[string]any{"reason_code": "admission_blocked", "reason": "newest webhook blocker"}}}}
				encoded, err := json.Marshal(record)
				if err != nil {
					t.Fatal(err)
				}
				lines = append(lines, string(encoded))
			}
			recordOutcomes(t, repository, strings.Join(lines, "\n"))
			c := &Controller{config: testControllerConfig()}
			c.config.RepositoryWorktree = repository
			request := c.incidentRequest(time.Now(), []sdk.Finding{{DetectorID: "health", Fingerprint: "missing:cfg", PrimaryResource: sdk.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "cfg"}}})
			// Use a valid public envelope while preserving the real controller projection.
			envelope := goldenRequest(t)
			envelope.PriorResponseProgress = request.PriorResponseProgress
			task, payload, err := encodeIncidentAgentTask(envelope, testIncidentRegistry(t, ""), testIncidentAgentID(t))
			if err != nil {
				t.Fatal(err)
			}
			if len(payload) > worker_registry.MaxRequestBytes {
				t.Fatal("oversized dispatch")
			}
			var typed contracts.IncidentRequest
			if err := task.Request.UnmarshalTo(&typed); err != nil {
				t.Fatal(err)
			}
			progress := typed.GetPriorResponseProgress()
			if progressWireBytes(progress) > maxResponseProgressBytes {
				t.Fatal("unbounded projection bytes")
			}
			if len(progress) == 0 || !progress[len(progress)-1].GetTruncated() || progress[len(progress)-1].GetOmittedEpisodes() != uint32(max(0, episodes-8)) {
				t.Fatalf("missing typed omissions: %v", progress)
			}
			if len(progress) > 8 {
				t.Fatalf("unbounded episode count: %d", len(progress))
			}
			found := false
			for _, item := range progress {
				for _, action := range item.SuccessfulActions {
					if action.ActionId == fmt.Sprintf("fixed-%03d", episodes-1) && len(item.Blockers) > 0 {
						found = true
					}
				}
			}
			if !found {
				t.Fatal("newest action/blocker lost")
			}
		})
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
