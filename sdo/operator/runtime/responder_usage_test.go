package runtime

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/memory"
)

// writeUsageCheckpoint stores one worker usage checkpoint the way the worker's UsageCheckpoint does.
func writeUsageCheckpoint(t *testing.T, mount, sessionID, turn string, complete bool, input, output, requests int) {
	t.Helper()
	sub, err := memory.IncidentSessionSubPath("", sessionID)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(mount, sub, "usage")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	document := fmt.Sprintf(`{"version":1,"incident_id":%q,"task_id":%q,"phase":"responder","turn_id":%q,"complete":%t,`+
		`"usage":{"llm_calls":%d,"input_tokens":%d,"output_tokens":%d,"cache_read_input_tokens":0,"cache_write_input_tokens":0,`+
		`"uncached_input_tokens":%d,"model_requests":%d,"total_cost_usd":0.5}}`,
		sessionID, sessionID, turn, complete, requests, input, output, input, requests)
	if err := os.WriteFile(filepath.Join(directory, turn+".json"), []byte(document), 0o644); err != nil {
		t.Fatal(err)
	}
}

// Investigators are separate sessions with their own usage checkpoints. The incident's accounting boundary sums
// them with the responder's: an eval that read only the responder would understate SDO's cost on exactly the
// slow incidents that started investigators.
func TestIncidentUsageIncludesTheInvestigatorSessions(t *testing.T) {
	mount := t.TempDir()
	writeUsageCheckpoint(t, mount, "inc-1", "r1", true, 1000, 100, 4)
	writeUsageCheckpoint(t, mount, investigatorID("inc-1", 1), "i1", true, 200, 20, 2)
	writeUsageCheckpoint(t, mount, investigatorID("inc-1", 2), "i2", true, 300, 30, 3)
	// another incident's investigator never counts
	writeUsageCheckpoint(t, mount, investigatorID("inc-2", 1), "x1", true, 9999, 999, 9)
	d := KubernetesJobDispatcher{RepositoryMountPath: mount, Investigators: &InvestigatorRegistry{Count: 2}}

	usage, err := d.ResponderUsage("inc-1")
	if err != nil {
		t.Fatal(err)
	}
	if usage == nil || usage.GetInputTokens() != 1500 || usage.GetOutputTokens() != 150 || usage.GetModelRequests() != 9 {
		t.Fatalf("incident usage = %v, want responder + both investigators (1500 in, 150 out, 9 requests)", usage)
	}
	if usage.GetPartial() {
		t.Fatalf("every turn completed, usage must not be partial: %v", usage)
	}
}

func TestIncidentUsageIsPartialWhenAnInvestigatorWasCutOff(t *testing.T) {
	mount := t.TempDir()
	writeUsageCheckpoint(t, mount, "inc-1", "r1", true, 1000, 100, 4)
	writeUsageCheckpoint(t, mount, investigatorID("inc-1", 1), "i1", false, 200, 20, 2)
	d := KubernetesJobDispatcher{RepositoryMountPath: mount, Investigators: &InvestigatorRegistry{Count: 2}}

	usage, err := d.ResponderUsage("inc-1")
	if err != nil {
		t.Fatal(err)
	}
	if usage.GetInputTokens() != 1200 || !usage.GetPartial() {
		t.Fatalf("a stopped investigator's last checkpoint counts and marks the total partial: %v", usage)
	}
}

func TestIncidentUsageWithoutInvestigatorsIsTheResponders(t *testing.T) {
	mount := t.TempDir()
	d := KubernetesJobDispatcher{RepositoryMountPath: mount, Investigators: &InvestigatorRegistry{Count: 2}}
	if usage, err := d.ResponderUsage("inc-1"); err != nil || usage != nil {
		t.Fatalf("nothing observed stays unobserved (nil), got %v, %v", usage, err)
	}
	writeUsageCheckpoint(t, mount, "inc-1", "r1", true, 1000, 100, 4)
	for _, registry := range []*InvestigatorRegistry{nil, {Count: 0}, {Count: 2}} {
		d.Investigators = registry
		usage, err := d.ResponderUsage("inc-1")
		if err != nil || usage.GetInputTokens() != 1000 || usage.GetPartial() {
			t.Fatalf("responder only: %v, %v", usage, err)
		}
	}
}

func TestReflectionUsageKeepsInterruptedTurnOutsideResponderTotals(t *testing.T) {
	mount := t.TempDir()
	const incidentID = "incident"
	writeUsageCheckpoint(t, mount, incidentID, "response", true, 1000, 100, 4)
	sub, err := memory.IncidentSessionSubPath("", incidentID)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(mount, sub, "usage", "reflect")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"version":1,"incident_id":"incident","task_id":"reflection-task","phase":"reflect","turn_id":"closing","complete":false,"usage":{"input_tokens":120,"output_tokens":10,"total_cost_usd":0.1}}`)
	if err := os.WriteFile(filepath.Join(dir, "closing.json"), document, 0o644); err != nil {
		t.Fatal(err)
	}
	response, _, err := memory.ResponderUsage(mount, "", incidentID)
	if err != nil || response.GetInputTokens() != 1000 || response.GetPartial() {
		t.Fatalf("responder usage = %v, err = %v", response, err)
	}
	reflection, _, err := memory.PhaseUsage(mount, "", incidentID, "reflect")
	if err != nil || reflection.GetInputTokens() != 120 || reflection.GetTotalCostUsd() != 0.1 || !reflection.GetPartial() {
		t.Fatalf("reflection usage = %v, err = %v", reflection, err)
	}
}
