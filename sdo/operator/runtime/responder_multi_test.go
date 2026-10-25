package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func multiCall(t *testing.T, loop *ResponderLoop, options string) ToolResponse {
	t.Helper()
	var request ToolRequest
	if err := json.Unmarshal([]byte(`{"command":"mitigate","selection_set":{"version":1,"options":`+options+`}}`), &request); err != nil {
		t.Fatal(err)
	}
	return loop.Handle(context.Background(), request)
}

func TestMultiMitigationIndependentOneRound(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	result := multiCall(t, loop, `[{"option_id":"SetField","parameters":{"ref":"Service/app/backend","path":"spec.ports[0].targetPort","value":"8080","reason":"restore route"}},{"option_id":"ScaleWorkload","parameters":{"ref":"Deployment/app/frontend","replicas":"1"}}]`)
	if result.Error != "" || result.Report == nil || len(result.MenuExecution) != 2 || loop.LoopsUsed() != 1 {
		t.Fatalf("batch failed: %+v", result)
	}
	if len(loop.MenuRepairActions()) != 2 {
		t.Fatal("per-option receipts missing")
	}
	if value, _ := testField(cluster.objects[menuService], "spec.ports.0.targetPort"); value != float64(8080) {
		t.Fatal(value)
	}
}

func TestMultiMitigationConflictBeforeWrites(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	before := len(cluster.patches)
	result := multiCall(t, loop, `[{"option_id":"ScaleWorkload","parameters":{"ref":"Deployment/app/frontend","replicas":"1"}},{"option_id":"RestartRollout","parameters":{"ref":"Deployment/app/frontend","reason":"restart"}}]`)
	if result.Report == nil || result.Report.ReasonCode != "write_set_conflict" || len(cluster.patches) != before || loop.LoopsUsed() != 0 {
		t.Fatalf("overlap not refused atomically: %+v writes=%d", result, len(cluster.patches)-before)
	}
}

func TestMultiMitigationPendingReadsAndOverlap(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	loop.Executor.Rollouts = peerBlockedRollout{entered: make(chan struct{}, 1)}
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	started := time.Now()
	result := multiCall(t, loop, `[{"option_id":"ScaleWorkload","parameters":{"ref":"Deployment/app/frontend","replicas":"1"}}]`)
	if time.Since(started) > time.Second || result.RolloutPending == nil || result.ApplicationID == "" {
		t.Fatalf("pending blocked: %+v", result)
	}
	for _, command := range []string{"status", "detect"} {
		if response := loop.Handle(context.Background(), ToolRequest{Command: command}); response.Error != "" {
			t.Fatalf("%s unavailable: %s", command, response.Error)
		}
	}
	overlap := multiCall(t, loop, `[{"option_id":"ScaleWorkload","parameters":{"ref":"Deployment/app/frontend","replicas":"2"}}]`)
	if overlap.Report == nil || overlap.Report.ReasonCode != "pending_write_conflict" || loop.LoopsUsed() != 1 {
		t.Fatalf("pending conflict not refused: %+v", overlap)
	}
	disjoint := multiCall(t, loop, `[{"option_id":"SetField","parameters":{"ref":"Service/app/backend","path":"spec.ports[0].targetPort","value":"8080","reason":"restore route"}}]`)
	if disjoint.Error != "" || loop.LoopsUsed() != 2 {
		t.Fatalf("disjoint repair refused: %+v", disjoint)
	}
}
