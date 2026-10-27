package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	programvalidation "sdo.dev/operator/memory/validate/program"
	"sdo.dev/operator/sdk/program"
)

func TestReadmeOnlyPlaybookDoesNotAutoApply(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "playbooks", "judgement")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("---\nschema_version: 1\nowner: responder\nfault_class: judgement\noriginating_incident: synthetic\n---\nInspect backend availability before choosing restoration or a scoped bypass.\n"), 0644); err != nil {
		t.Fatal(err)
	}
	specs, err := programvalidation.InspectMemory(root, "synthetic-memory")
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 0 {
		t.Fatalf("README-only memory produced executable constructors: %v", specs)
	}
	// The production registry has no constructor to register for this memory.
	registry, err := program.NewRegistry(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	loop, state := pipelineTestLoop(t, true)
	loop.Programs = registry
	loop.Request.SurfacedPlaybooks = []SurfacedPlaybook{{PlaybookID: "judgement"}}
	loop.Request.InitialProgramRuns = nil
	loop.Request.PlaybookCoverage = nil
	state.Request = loop.Request
	state.Coverage = nil
	before := findingFingerprints(state.Findings)
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Mitigations) != 0 || len(loop.PlanApplications()) != 0 || len(state.Findings) != len(before) {
		t.Fatalf("README guidance auto-applied or cleared findings: %v %+v", report, state.Findings)
	}
}
