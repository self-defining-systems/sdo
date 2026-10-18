package memory

import (
	"bytes"
	"testing"
)

func TestWorkerWireProjectsPhysicalReferencesToLogicalIDs(t *testing.T) {
	input := []byte(`{"applied_playbooks":[{"path":".sdo/playbooks/cache-pressure/README.md","scripts":[".sdo/playbooks/cache-pressure/scripts/diagnose.sh"]}],"proposed_memory_changes":[".sdo/outcomes.jsonl",".sdo/playbooks/cache-pressure/README.md"]}`)
	output, err := NormalizeWorkerWire(input)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(output, []byte(".sdo/")) || !bytes.Contains(output, []byte(`"playbook_id":"cache-pressure"`)) || !bytes.Contains(output, []byte(`"script_names":["diagnose.sh"]`)) {
		t.Fatalf("worker payload did not contain logical IDs: %s", output)
	}
}

func TestRequiresControllerRolloutClassifiesAcceptedPaths(t *testing.T) {
	if !RequiresControllerRollout([]string{".sdo/diagnostics/manifest.yaml"}) {
		t.Fatal("manifest edit must restart controller")
	}
	if !RequiresControllerRollout([]string{".sdo/diagnostics/detectors/incidents/cache/detector.go"}) {
		t.Fatal("incident detector edit must restart controller")
	}
	if RequiresControllerRollout([]string{".sdo/playbooks/cache-pressure/README.md"}) {
		t.Fatal("playbook edit alone must not restart controller")
	}
}
