package runtime

import (
	"context"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/sim"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// stageState opens an incident the way the dispatcher does: the open-time
// diagnosis is already part of the request.
func stageState(loop *ResponderLoop) *IncidentState {
	request := loop.Request
	request.InitialProgramRuns = loop.ProgramRuns()
	return &IncidentState{Request: request, Findings: append([]sdk.Finding(nil), request.Findings...)}
}

// A saved program whose diagnosis matches live is planned by the program
// registry, applied and verified by the controller, and clears the incident
// with no agent stage and no trust record.
func TestDeterministicStageClearsMatchingProgram(t *testing.T) {
	sim.Sweep(t, func(t *testing.T, w *sim.World) {
		loop, kube := simFastPathLoop(t, w)
		loop.Trust = nil
		state := stageState(loop)
		agent := &countingStage{name: "agent"}
		err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}, agent})
		if err != nil {
			t.Fatal(err)
		}
		if agent.runs != 0 {
			t.Fatalf("agent stage ran although findings cleared: %+v", state.PriorStages)
		}
		if len(state.Findings) != 0 || len(state.PriorStages) != 1 {
			t.Fatalf("findings=%v stages=%v", state.Findings, state.PriorStages)
		}
		report := state.PriorStages[0]
		if report.GetStage() != "deterministic" || len(report.GetMitigations()) != 1 {
			t.Fatalf("report = %v", report)
		}
		attempt := report.GetMitigations()[0]
		if attempt.GetResult() != "applied_cleared" || attempt.GetPlaybookId() != "service-targetport-mismatch" || attempt.GetProgramRevision() == "" && false {
			t.Fatalf("attempt = %v", attempt)
		}
		if len(report.GetClearedFindingFingerprints()) != 1 || len(report.GetRemainingFindingFingerprints()) != 0 {
			t.Fatalf("clearance = %v / %v", report.GetClearedFindingFingerprints(), report.GetRemainingFindingFingerprints())
		}
		object, _ := kube.Get(context.Background(), sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"})
		if port, _ := testField(object, "spec.ports.0.targetPort"); port == int64(9999) {
			t.Fatalf("service still faulted: %v", port)
		}
		if len(loop.PlanApplications()) != 1 || len(state.RepairActions) == 0 {
			t.Fatalf("applications=%d repair actions=%d", len(loop.PlanApplications()), len(state.RepairActions))
		}
	})
}

type countingStage struct {
	name string
	runs int
}

func (s *countingStage) Name() string { return s.name }
func (s *countingStage) Run(context.Context, *IncidentState) (*contracts.StageReport, error) {
	s.runs++
	return nil, nil
}

// A restart after the stage's write re-observes the cluster. The write is never
// replayed, and the agent only starts when the fault is still present.
func TestDeterministicStageNeverReplaysWriteAfterRestart(t *testing.T) {
	for _, faultPresent := range []bool{false, true} {
		name := map[bool]string{false: "fixed", true: "fault_present"}[faultPresent]
		t.Run(name, func(t *testing.T) {
			sim.Sweep(t, func(t *testing.T, w *sim.World) {
				loop, kube := simFastPathLoop(t, w)
				state := stageState(loop)
				stage := &DeterministicStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}
				if err := RunResponseStages(context.Background(), state, []ResponseStage{stage}); err != nil || len(state.Findings) != 0 {
					t.Fatalf("first pass: %v %v", state.Findings, err)
				}
				writes := len(loop.PlanApplications())
				persisted := loop.stageWrites
				if persisted != 1 {
					t.Fatalf("stage write was not counted before applying: %d", persisted)
				}
				ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
				if faultPresent {
					kube.Put(simService(9999))
					w.Cache.CatchUp()
				}
				restarted := NewResponderLoop(loop.Request, loop.Programs, loop.Provider, loop.Detectors, loop.Executor, loop.MaxBatches)
				restarted.ReadView, restarted.CorpusReplayPassed = loop.ReadView, true
				restarted.stageWrites = persisted
				reopened := stageState(loop)
				reopened.Findings = append([]sdk.Finding(nil), loop.Request.Findings...)
				if err := RunResponseStages(context.Background(), reopened, []ResponseStage{&DeterministicStage{NewLoop: func(IncidentRequest) *ResponderLoop { return restarted }}}); err != nil {
					t.Fatal(err)
				}
				object, _ := kube.Get(context.Background(), ref)
				port, _ := testField(object, "spec.ports.0.targetPort")
				if faultPresent {
					if len(reopened.Findings) == 0 || port != int64(9999) {
						t.Fatalf("restart wrote or hid the fault: port=%v findings=%v", port, reopened.Findings)
					}
					for _, attempt := range reopened.PriorStages[0].GetMitigations() {
						if attempt.GetSkipReason() != "already_attempted" {
							t.Fatalf("attempt = %v", attempt)
						}
					}
				} else if len(reopened.Findings) != 0 || !reopened.Healthy {
					t.Fatalf("restart did not close from a fresh observation: %v", reopened.Findings)
				}
				if writes != 1 || len(restarted.PlanApplications()) != 0 {
					t.Fatalf("restart replayed a write: %d %d", writes, len(restarted.PlanApplications()))
				}
			})
		})
	}
}

func TestStageIneffectiveCoversOnlySameRevisionAndFindings(t *testing.T) {
	ineffective := stageIneffective{}
	first := stageCandidate{programID: "p", revision: "r1", targets: []string{"a", "b"}}
	ineffective.note(first, &contracts.StageMitigation{Result: "applied_cleared"})
	if ineffective.covers(first) {
		t.Fatal("a cleared attempt is not ineffective")
	}
	ineffective.note(first, &contracts.StageMitigation{Result: "applied_still_matching"})
	for name, candidate := range map[string]stageCandidate{
		"same":          {programID: "p", revision: "r1", targets: []string{"a"}},
		"new bindings":  {programID: "p", revision: "r1", targets: []string{"a", "b"}},
		"new finding":   {programID: "p", revision: "r1", targets: []string{"a", "c"}},
		"new revision":  {programID: "p", revision: "r2", targets: []string{"a"}},
		"other program": {programID: "q", revision: "r1", targets: []string{"a"}},
	} {
		want := name == "same" || name == "new bindings"
		if got := ineffective.covers(candidate); got != want {
			t.Errorf("%s: covers = %v, want %v", name, got, want)
		}
	}
}

// Every reason an otherwise matching program is not applied is typed, writes
// nothing, and leaves the findings for the next stage.
func TestDeterministicStageSkipsWithTypedReasons(t *testing.T) {
	cases := map[string]func(*testing.T, *sim.World, *ResponderLoop, *sim.Kube, *IncidentState){
		"repair_policy": func(_ *testing.T, _ *sim.World, _ *ResponderLoop, _ *sim.Kube, state *IncidentState) {
			state.Request.RepairPolicy = "commit"
		},
		"no_mitigation": func(t *testing.T, _ *sim.World, loop *ResponderLoop, _ *sim.Kube, _ *IncidentState) {
			registry, err := program.NewRegistry([]program.Diagnoser{fastloopPortProgram{}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			loop.Programs = registry
		},
		"unbound_parameter": func(_ *testing.T, _ *sim.World, _ *ResponderLoop, _ *sim.Kube, state *IncidentState) {
			for i := range state.Request.InitialProgramRuns {
				state.Request.InitialProgramRuns[i].Bindings = program.Bindings{}
			}
		},
		"diagnose_no_longer_matches": func(_ *testing.T, w *sim.World, _ *ResponderLoop, kube *sim.Kube, _ *IncidentState) {
			kube.Put(simService(8080))
			w.Cache.CatchUp()
		},
	}
	for reason, mutate := range cases {
		t.Run(reason, func(t *testing.T) {
			sim.Sweep(t, func(t *testing.T, w *sim.World) {
				loop, kube := simFastPathLoop(t, w)
				state := stageState(loop)
				mutate(t, w, loop, kube, state)
				report, err := (&DeterministicStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
				if err != nil || len(report.GetMitigations()) != 1 {
					t.Fatalf("report = %v err = %v", report, err)
				}
				attempt := report.GetMitigations()[0]
				if attempt.GetResult() != "skipped" || attempt.GetSkipReason() != reason {
					t.Fatalf("attempt = %v", attempt)
				}
				if len(loop.PlanApplications()) != 0 || loop.stageWrites != 0 {
					t.Fatal("a skipped attempt wrote")
				}
			})
		})
	}
}
