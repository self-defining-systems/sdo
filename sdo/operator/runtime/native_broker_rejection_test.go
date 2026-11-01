package runtime

import (
	"context"
	"os"
	"path/filepath"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
)

type rejectionHarness struct {
	t         *testing.T
	target    string
	native    NativeIncidentBroker
	workspace IncidentWorkspace
	closure   IncidentClosure
	calls     []ReflectionFeedback
}

func (h *rejectionHarness) setReflector(behave func(call int, w IncidentWorkspace) (ReflectionDecision, error)) {
	h.native.Reflector = ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, w IncidentWorkspace, _ *contracts.OutcomeRecord, _, _ string, feedback ReflectionFeedback) (ReflectionDecision, error) {
		h.calls = append(h.calls, feedback)
		return behave(len(h.calls), w)
	})
}

func writeBadPlaybook(w IncidentWorkspace) (ReflectionDecision, error) {
	return ReflectionDecision{LearningDecision: "updated", PlaybookIDs: []string{"base"}}, os.WriteFile(filepath.Join(w.Worktree, ".sdo", "playbooks", "base", "README.md"), []byte("bad playbook\n"), 0o644)
}

func writeGoodPlaybook(w IncidentWorkspace) (ReflectionDecision, error) {
	body := "---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: initial\n---\nRepair <RESOURCE_NAME> carefully\n"
	return ReflectionDecision{LearningDecision: "updated", PlaybookIDs: []string{"base"}}, os.WriteFile(filepath.Join(w.Worktree, ".sdo", "playbooks", "base", "README.md"), []byte(body), 0o644)
}

func TestMaxValidationRetriesIsTwo(t *testing.T) {
	if broker.MaxValidationRetries != 2 {
		t.Fatalf("MaxValidationRetries = %d, want 2", broker.MaxValidationRetries)
	}
}
