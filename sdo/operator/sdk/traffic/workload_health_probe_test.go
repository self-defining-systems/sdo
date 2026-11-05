package traffic

import "testing"

// A health probe may run only write scenarios: the detection designer keeps
// write-then-read-back probes in a workload of their own, and the read-back is
// a step of the write scenario itself.
func TestHealthProbeMayHoldOnlyWriteScenarios(t *testing.T) {
	catalog := Catalog{{
		ID: "write-item", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectIdempotentWrite, Marker: "sdo-synthetic",
	}}
	workload := Workload{
		APIVersion: APIVersion, Kind: WorkloadKind, Name: "probe-write", Purpose: PurposeHealthProbe,
		Scenarios: []WorkloadScenario{{ID: "write-item"}},
	}.WithDefaults()
	if err := workload.ValidateAgainst(catalog); err != nil {
		t.Fatalf("a health probe of write scenarios was rejected: %v", err)
	}
	workload.Scenarios = []WorkloadScenario{{ID: "missing"}}
	if err := workload.ValidateAgainst(catalog); err == nil {
		t.Fatal("a scenario the generators do not provide was accepted")
	}
}
