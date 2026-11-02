package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestResponderProgramCapabilitySurvivesToolAndContractViews(t *testing.T) {
	for _, withMitigator := range []bool{false, true} {
		t.Run(map[bool]string{false: "diagnosis-only", true: "mitigator"}[withMitigator], func(t *testing.T) {
			var mitigators []program.Mitigator
			if withMitigator {
				mitigators = []program.Mitigator{fastloopPortProgram{}}
			}
			registry, err := program.NewRegistry([]program.Diagnoser{fastloopPortProgram{}}, mitigators)
			if err != nil {
				t.Fatal(err)
			}
			findings := []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}}}
			runs := SelectProgramCandidates(context.Background(), registry, &sdktest.ReplayReadView{}, findings, time.Second)
			if len(runs) != 1 {
				t.Fatalf("expected one run: %+v", runs)
			}
			loop := NewResponderLoop(IncidentRequest{IncidentID: "capability", Namespace: "app"}, registry, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, nil, PlanExecutor{}, 5)
			loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
				return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
			}
			response := loop.Handle(context.Background(), ToolRequest{Command: "diagnose", ProgramID: "service-targetport-mismatch", Bindings: program.Bindings{Objects: program.ObjectBindings{"service": findings[0].PrimaryResource}}})
			if response.Error != "" || len(response.ProgramRuns) != 1 {
				t.Fatalf("explicit diagnosis failed: %+v", response)
			}
			for _, run := range []ProgramRun{runs[0], programRunState(programRunContract(runs[0])), response.ProgramRuns[0]} {
				encoded, err := json.Marshal(run)
				if err != nil {
					t.Fatal(err)
				}
				var view map[string]any
				if err := json.Unmarshal(encoded, &view); err != nil {
					t.Fatal(err)
				}
				if value, ok := view["can_mitigate"].(bool); !ok || value != withMitigator {
					t.Fatalf("capability missing or wrong in tool/contract view: %s", encoded)
				}
			}
		})
	}
}

func TestLegacyProgramRunCapabilityRemainsUnknown(t *testing.T) {
	run := ProgramRun{ProgramID: "legacy", Verdict: program.Verdict{Result: program.Unknown}}
	projected := programRunContract(run)
	if projected.CanMitigate != nil || programRunState(projected).CanMitigate != nil {
		t.Fatal("absent capability was converted into diagnosis-only")
	}
}
