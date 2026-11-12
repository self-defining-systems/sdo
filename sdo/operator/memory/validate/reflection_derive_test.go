package validate

import (
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestDeriveReflectionDecisionFromTheDiff(t *testing.T) {
	playbook := []string{".sdo/playbooks/fix/README.md", ".sdo/playbooks/fix/diagnose.go", ".sdo/playbooks/other/mitigate.go", ".sdo/corpus/case-1/labels.json"}
	got := DeriveReflectionDecision(nil, playbook)
	if got.GetLearningDecision() != "updated" || len(got.GetPlaybookIds()) != 2 || got.GetPlaybookIds()[0] != "fix" || got.GetPlaybookIds()[1] != "other" {
		t.Fatalf("decision = %v, want updated for fix and other", got)
	}
	if diagnostics := ReflectionDiagnostics(got, playbook); len(diagnostics) != 0 {
		t.Fatalf("a derived decision must pass the diff rules: %v", diagnostics)
	}
	// Evidence Go wrote with no playbook change is still an update, with no IDs.
	evidence := []string{".sdo/corpus/case-1/labels.json"}
	if got := DeriveReflectionDecision(nil, evidence); got.GetLearningDecision() != "updated" || len(got.GetPlaybookIds()) != 0 {
		t.Fatalf("evidence-only decision = %v", got)
	}
	empty := DeriveReflectionDecision(nil, nil)
	if empty.GetLearningDecision() != "no_change" || empty.GetNoChangeReason() == "" || len(ReflectionDiagnostics(empty, nil)) != 0 {
		t.Fatalf("empty diff decision = %v", empty)
	}
}

func TestDeriveReflectionDecisionKeepsAnExplicitDecision(t *testing.T) {
	explicit := &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "already covered"}
	if got := DeriveReflectionDecision(explicit, []string{".sdo/playbooks/fix/README.md"}); got != explicit {
		t.Fatalf("an explicit decision was replaced: %v", got)
	}
	// A reason the agent gave survives when only the verdict is derived.
	reasoned := DeriveReflectionDecision(&contracts.ReflectionDecision{NoChangeReason: "nothing worth a playbook"}, nil)
	if reasoned.GetLearningDecision() != "no_change" || reasoned.GetNoChangeReason() != "nothing worth a playbook" {
		t.Fatalf("reasoned = %v", reasoned)
	}
}
