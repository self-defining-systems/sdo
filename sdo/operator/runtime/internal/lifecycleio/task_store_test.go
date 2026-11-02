package lifecycleio

import (
	"os"
	"testing"
)

// A reflection task frozen by an earlier build carries retired field names and
// values inside its request. A restarted controller must still resume it.
func TestFrozenReflectionTaskFromEarlierBuildLoads(t *testing.T) {
	store := TaskStore{Root: t.TempDir()}
	const id = "reflection-1"
	document := `{"schema_version":"sdo.dev/v1alpha1","task_id":"reflection-1","agent_id":"a","task_type":"reflection","request":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.ReflectionRequest","outcome":{"schema_version":1,"incident_id":"i","classification":"success","reflection_eligibility":{"version":1,"evidence_gap_count":2,"unassigned":[{"program_id":"p","disposition":"question","reason":"evidence_gap"}]}}}}`
	if err := os.WriteFile(store.taskPath(id), []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	task, found, err := store.Load(id)
	if err != nil || !found || task.GetTaskId() != id {
		t.Fatalf("frozen task from an earlier build did not load: %v %v %v", task, found, err)
	}
}
