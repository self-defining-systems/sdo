package runtime

import (
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/memory"
)

// A usage shape problem is accounting evidence, never a reason to fail the incident: an invalid checkpoint is
// dropped, the total is marked partial and the problem is reported.
func TestInvalidUsageCheckpointDegradesToPartialUsage(t *testing.T) {
	mount := t.TempDir()
	writeUsageCheckpoint(t, mount, "inc-1", "r1", true, 1000, 100, 4)
	sub, err := memory.IncidentSessionSubPath("", "inc-1")
	if err != nil {
		t.Fatal(err)
	}
	bad := `{"version":1,"incident_id":"inc-1","task_id":"inc-1","phase":"responder","turn_id":"r2","complete":true,` +
		`"usage":{"llm_calls":1,"input_tokens":10,"output_tokens":1,"cache_read_input_tokens":20}}`
	if err := os.WriteFile(filepath.Join(mount, sub, "usage", "r2.json"), []byte(bad), 0o644); err != nil {
		t.Fatal(err)
	}
	var reported []error
	d := KubernetesJobDispatcher{RepositoryMountPath: mount, OnUsageProblem: func(err error) { reported = append(reported, err) }}
	usage, err := d.ResponderUsage("inc-1")
	if err != nil {
		t.Fatalf("usage problem must not fail the incident: %v", err)
	}
	if usage == nil || usage.GetInputTokens() != 1000 || !usage.GetPartial() {
		t.Fatalf("usage = %v, want the valid checkpoint, marked partial", usage)
	}
	if len(reported) != 1 {
		t.Fatalf("reported = %v, want one usage problem", reported)
	}
}
