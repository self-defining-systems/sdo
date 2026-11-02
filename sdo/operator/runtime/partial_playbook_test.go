package runtime

import (
	"context"
	"k8s.io/apimachinery/pkg/util/intstr"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"

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

// A still-matching diagnosis is information after the first apply. The stage
// must retain its attempt across later passes even while the fault is observed.
func TestStillMatchingProgramIsNotAppliedTwiceWithinIncident(t *testing.T) {
	ctx := context.Background()
	loop, state := pipelineTestLoop(t, true)
	loop.Provider = stillFaultedStageView{loop.Provider}
	state.Coverage = nil
	state.Request.PlaybookCoverage = nil
	state.Request.InitialProgramRuns = nil
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true, ObservationCeiling: time.Millisecond}
	first, err := stage.Run(ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	state.PriorStages = []*contracts.StageReport{first}
	if len(loop.PlanApplications()) != 1 {
		t.Fatalf("fixture did not apply: %v", first)
	}
	matched := false
	for _, attempt := range first.Mitigations {
		matched = matched || attempt.GetPostApplyDiagnose().GetVerdict().GetResult() == contracts.Verdict_RESULT_MATCH
	}
	if !matched {
		t.Fatalf("fixture lost still-matching diagnosis: %v", first)
	}
	for range 2 {
		if _, err := RunStageAgain(ctx, state, stage); err != nil {
			t.Fatal(err)
		}
	}
	if len(loop.PlanApplications()) != 1 {
		t.Fatalf("still-matching program applied twice: %v", loop.PlanApplications())
	}
}

type stillFaultedStageView struct{ SnapshotProvider }

func (v stillFaultedStageView) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	view, err := v.SnapshotProvider.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	snapshot := view.(sdktest.Snapshot)
	for index := range snapshot.ServiceList {
		if snapshot.ServiceList[index].Name == "api" {
			snapshot.ServiceList[index].Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
		}
	}
	return snapshot, nil
}
