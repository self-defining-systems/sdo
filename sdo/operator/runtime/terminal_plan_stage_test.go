package runtime

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type terminalShapeProgram struct {
	pipelinePortProgram
	shape string
	calls *atomic.Int32
}

func (p terminalShapeProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	p.calls.Add(1)
	if p.shape == "empty" {
		return program.Plan{}, nil
	}
	return program.Plan{Actions: []program.Action{program.PatchObject{Ref: b.Objects["service"], Type: "merge", Patch: []byte(`{}`), Edits: []program.ListEdit{{Path: "/spec/ports", Match: map[string]any{"name": "http"}, Op: "remove"}}, Reason: "synthetic invalid shape"}}}, nil
}
func TestAutomaticStageNonExecutablePlanIsTerminalAcrossBindingsAndRestart(t *testing.T) {
	for _, shape := range []string{"empty", "exclusive-patch-edits"} {
		t.Run(shape, func(t *testing.T) {
			ctx := context.Background()
			loop, state := pipelineTestLoopWithServices(t, true, "peer")
			var calls atomic.Int32
			p := terminalShapeProgram{shape: shape, calls: &calls}
			registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
			if err != nil {
				t.Fatal(err)
			}
			loop.Programs = registry
			state.Coverage = nil
			state.Request.InitialProgramRuns = nil
			loop.Request = state.Request
			opening := append([]sdk.Finding(nil), state.Findings...)
			var durable []byte
			loop.saveToolState = func(_ context.Context, b []byte) error { durable = append([]byte(nil), b...); return nil }
			stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true}
			dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
			if err := RunResponseStages(ctx, state, []ResponseStage{stage, AgentResponseStage{Dispatcher: dispatcher}}); err != nil {
				t.Fatal(err)
			}
			request := <-dispatcher.requests
			if calls.Load() != 1 {
				t.Fatalf("non-executable program was retried across live bindings: %d calls", calls.Load())
			}
			if len(request.PriorStages) != 1 || len(request.PriorStages[0].Mitigations) != 2 {
				t.Fatalf("missing terminal handoff evidence: %+v", request)
			}
			for _, attempt := range request.PriorStages[0].Mitigations {
				if attempt.Result != "skipped" || attempt.SkipReason != "plan_not_executable" || attempt.Error == "" || attempt.ApplyReceipt != nil {
					t.Fatalf("terminal reason/receipt: %v", attempt)
				}
			}
			if err := wire.Validate(request.PriorStages[0]); err != nil {
				t.Fatal(err)
			}
			if len(loop.PlanApplications()) != 0 {
				t.Fatal("invalid shape wrote")
			}
			state.Findings = opening
			if _, err := RunStageAgain(ctx, state, stage); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Fatalf("refresh retried a terminal plan: %d", calls.Load())
			}
			fresh := NewResponderLoop(loop.Request, registry, loop.Provider, loop.Detectors, loop.Executor, 5)
			fresh.ReadView = loop.ReadView
			if err := fresh.restoreToolSnapshot(durable); err != nil {
				t.Fatal(err)
			}
			selected := request.PriorStages[0].Mitigations[0]
			response := fresh.Handle(ctx, ToolRequest{Command: "plan", ProgramID: selected.PlaybookId, Bindings: programBindingsState(selected.Bindings)})
			if response.Error == "" || calls.Load() != 1 {
				t.Fatalf("tool rejoin retried terminal program: calls=%d response=%+v", calls.Load(), response)
			}
			resumed := &IncidentState{Request: loop.Request, Findings: opening, Refresh: true}
			if _, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return fresh }, KeepLoop: true}).Run(ctx, resumed); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 || len(fresh.PlanApplications()) != 0 {
				t.Fatalf("restart retried terminal program: calls=%d applications=%v", calls.Load(), fresh.PlanApplications())
			}
		})
	}
}

type transientPlanProgram struct {
	pipelinePortProgram
	calls *atomic.Int32
}

func (p transientPlanProgram) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	if p.calls.Add(1) == 1 {
		return program.Plan{}, fmt.Errorf("live observation unavailable")
	}
	return p.pipelinePortProgram.Mitigate(ctx, v, b)
}
func TestAutomaticStageTransientPlanningErrorIsNotTerminal(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	var calls atomic.Int32
	p := transientPlanProgram{calls: &calls}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry
	state.Coverage = nil
	state.Request.InitialProgramRuns = nil
	loop.Request = state.Request
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || len(report.Mitigations) != 1 || report.Mitigations[0].Result != "applied_cleared" {
		t.Fatalf("transient error was mistaken for a structural defect: calls=%d report=%v", calls.Load(), report)
	}
}
