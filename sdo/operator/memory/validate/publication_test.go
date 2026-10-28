package validate

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestLoadReflectionDecisionAcceptsTheReturnedResultShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "decision.json")
	// Reflection copies its returned ReflectionResult as the decision file.
	body := `{"summary":"reused program","learning_decision":"updated","no_change_reason":"","proposed_artifact_ids":["base"],"sdk_gaps":[]}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	decision, err := LoadReflectionDecision(context.Background(), t.TempDir(), path)
	if err != nil {
		t.Fatalf("decision with summary rejected: %v", err)
	}
	if decision.GetLearningDecision() != "updated" || len(decision.GetPlaybookIds()) != 1 {
		t.Fatalf("decision = %v", decision)
	}
}

func TestLoadReflectionDecisionStillRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "decision.json")
	if err := os.WriteFile(path, []byte(`{"learning_decision":"updated","playbook_ids":["base"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadReflectionDecision(context.Background(), t.TempDir(), path)
	if err == nil || !strings.Contains(err.Error(), "playbook_ids") {
		t.Fatalf("misspelled field accepted or misreported: %v", err)
	}
}

// A no_change decision over files Go (or the agent) already changed names the
// exact decision to return instead.
func TestNoChangeOverChangedFilesNamesTheUpdatedDecision(t *testing.T) {
	paths := []string{".sdo/corpus/settled-case/labels.json", ".sdo/playbooks/source-drift/README.md", ".sdo/playbooks/webhook-ca/diagnose.go"}
	ds := ReflectionDiagnostics(&contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "covered"}, paths)
	if len(ds) != 1 || !strings.Contains(ds[0].Hint, `learning_decision "updated"`) || !strings.Contains(ds[0].Hint, `["source-drift","webhook-ca"]`) {
		t.Fatalf("no_change over changed files: %+v", ds)
	}
	ds = ReflectionDiagnostics(&contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "covered"}, paths[:1])
	if len(ds) != 1 || !strings.Contains(ds[0].Hint, `proposed_artifact_ids []`) {
		t.Fatalf("corpus-only change: %+v", ds)
	}
}
