package validate

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadReflectionDecisionAcceptsTheReturnedResultShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "decision.json")
	// Reflection copies its returned ReflectionTurn, which carries a summary.
	body := `{"summary":"reused program","learning_decision":"updated","playbook_ids":["base"],"sdk_gaps":[]}`
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
	if err := os.WriteFile(path, []byte(`{"learning_decision":"updated","playbook_id":["base"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadReflectionDecision(context.Background(), t.TempDir(), path)
	if err == nil || !strings.Contains(err.Error(), "playbook_id") {
		t.Fatalf("misspelled field accepted or misreported: %v", err)
	}
}
