//go:build integration

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
