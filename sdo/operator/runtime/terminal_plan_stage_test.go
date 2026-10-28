package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sdo.dev/operator/builder"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type terminalShapeProgram struct {
	pipelinePortProgram
	shape    string
	calls    *atomic.Int32
	compiled program.Mitigator
}

func (p terminalShapeProgram) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	p.calls.Add(1)
	if p.compiled != nil {
		return p.compiled.Mitigate(ctx, v, b)
	}
	if p.shape == "empty" {
		return program.Plan{}, nil
	}
	return program.Plan{Actions: []program.Action{program.PatchObject{Ref: b.Objects["service"], Type: "merge", Patch: []byte(`{}`), Edits: []program.ListEdit{{Path: "/spec/ports", Match: map[string]any{"name": "http"}, Op: "remove"}}, Reason: "synthetic invalid shape"}}}, nil
}
func TestAutomaticStageNonExecutablePlanIsTerminalPerBindingAndRestart(t *testing.T) {
	for _, shape := range []string{"empty", "exclusive-patch-edits", "compiled-malformed-json", "compiled-nil-action"} {
		t.Run(shape, func(t *testing.T) {
			ctx := context.Background()
			loop, state := pipelineTestLoopWithServices(t, true, "peer")
			var calls atomic.Int32
			p := terminalShapeProgram{shape: shape, calls: &calls}
			if strings.HasPrefix(shape, "compiled-") {
				action := `program.PatchObject{Ref:b.Objects["service"],Type:"merge",Patch:[]byte("{"),Reason:"synthetic"}`
				if shape == "compiled-nil-action" {
					action = "nil"
				}
				p.compiled = compiledTerminalPlanProgram(t, `return program.Plan{Actions:[]program.Action{`+action+`}},nil`)

			}
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
			if calls.Load() != 2 {
				t.Fatalf("each non-executable binding needs one attempt: %d calls", calls.Load())
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
			if calls.Load() != 2 {
				t.Fatalf("refresh retried a terminal plan: %d", calls.Load())
			}
			fresh := NewResponderLoop(loop.Request, registry, loop.Provider, loop.Detectors, loop.Executor, 5)
			fresh.ReadView = loop.ReadView
			if err := fresh.restoreToolSnapshot(durable); err != nil {
				t.Fatal(err)
			}
			selected := request.PriorStages[0].Mitigations[0]
			response := fresh.Handle(ctx, ToolRequest{Command: "plan", ProgramID: selected.PlaybookId, Bindings: programBindingsState(selected.Bindings)})
			if response.Error == "" || calls.Load() != 2 {
				t.Fatalf("tool rejoin retried terminal program: calls=%d response=%+v", calls.Load(), response)
			}
			resumed := &IncidentState{Request: loop.Request, Findings: opening, Refresh: true}
			if _, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return fresh }, KeepLoop: true}).Run(ctx, resumed); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 2 || len(fresh.PlanApplications()) != 0 {
				t.Fatalf("restart retried terminal program: calls=%d applications=%v", calls.Load(), fresh.PlanApplications())
			}
		})
	}
}

// A phantom webhook binding returned an empty plan before the real binding.
// Sweep which binding is empty so traversal order cannot grant shared failure.
func TestAutomaticStageEmptyPlanDoesNotPoisonAnotherBinding(t *testing.T) {
	for _, empty := range []string{"api", "peer"} {
		t.Run(empty, func(t *testing.T) {
			loop, state := pipelineTestLoopWithServices(t, true, "peer")
			var calls atomic.Int32
			p := bindingShapeProgram{empty: empty, calls: &calls}
			registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
			if err != nil {
				t.Fatal(err)
			}
			loop.Programs, loop.Request = registry, state.Request
			var durable []byte
			loop.saveToolState = func(_ context.Context, data []byte) error { durable = append([]byte(nil), data...); return nil }
			state.Coverage, state.Request.InitialProgramRuns = nil, nil
			report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Mitigations) != 2 || len(loop.PlanApplications()) != 1 {
				t.Fatalf("empty binding poisoned a peer: %v", report)
			}
			for _, attempt := range report.Mitigations {
				name := programBindingsState(attempt.Bindings).Objects["service"].Name
				if name == empty {
					if attempt.SkipReason != stagePlanNotExecutable {
						t.Fatalf("empty binding must stay terminal: %v", attempt)
					}
				} else if attempt.Result != "applied_cleared" {
					t.Fatalf("valid binding must execute: %v", attempt)
				}
			}
			fresh := NewResponderLoop(loop.Request, registry, loop.Provider, loop.Detectors, loop.Executor, 5)
			fresh.ReadView = loop.ReadView
			if err := fresh.restoreToolSnapshot(durable); err != nil {
				t.Fatal(err)
			}
			before := calls.Load()
			for _, attempt := range report.Mitigations {
				bindings := programBindingsState(attempt.Bindings)
				if bindings.Objects["service"].Name != empty {
					continue
				}
				response := fresh.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: p.Spec().ID, Bindings: bindings})
				if response.Error == "" || calls.Load() != before {
					t.Fatal("restart retried the same terminal binding")
				}
			}
			for _, attempt := range report.Mitigations {
				bindings := programBindingsState(attempt.Bindings)
				if bindings.Objects["service"].Name == empty {
					continue
				}
				response := fresh.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: p.Spec().ID, Bindings: bindings})
				if response.Error != "" || response.PlanID == "" || calls.Load() != before+1 {
					t.Fatalf("restored failure poisoned another binding: %+v", response)
				}
			}
		})
	}
}

type bindingShapeProgram struct {
	pipelinePortProgram
	empty string
	calls *atomic.Int32
}

func (p bindingShapeProgram) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	p.calls.Add(1)
	if b.Objects["service"].Name == p.empty {
		return program.Plan{}, nil
	}
	return p.pipelinePortProgram.Mitigate(ctx, v, b)
}

func TestTerminalPlanFailureIsScopedToScalarBindingValues(t *testing.T) {
	loop, _ := pipelineTestLoop(t, true)
	p := scalarShapeProgram{}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry
	bindings := program.Bindings{Objects: program.ObjectBindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}, Values: map[string]any{"executable": false}}
	first := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: p.Spec().ID, Bindings: bindings})
	bindings.Values["executable"] = true
	second := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: p.Spec().ID, Bindings: bindings})
	if first.Error == "" || second.Error != "" || second.PlanID == "" {
		t.Fatalf("scalar input was omitted from failure scope: first=%+v second=%+v", first, second)
	}
}

type scalarShapeProgram struct{ pipelinePortProgram }

func (scalarShapeProgram) Spec() program.ProgramSpec {
	spec := (pipelinePortProgram{}).Spec()
	spec.Params = append(spec.Params, program.ParamSpec{Name: "executable", ValueKind: program.BoolValue, Default: false})
	return spec
}
func (p scalarShapeProgram) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	if b.Values["executable"] != true {
		return program.Plan{}, nil
	}
	return p.pipelinePortProgram.Mitigate(ctx, v, b)
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

// Compile real authored source so encoding and read errors cross private pipes.
func compiledTerminalPlanProgram(t *testing.T, body string) *workingProgram {
	t.Helper()
	root := nativeRepository(t)
	spec := (pipelinePortProgram{}).Spec()
	dir := filepath.Join(root, ".sdo/playbooks", spec.ID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("---\nfault_class: "+spec.ID+"\n---\n# Synthetic structural defect\n"), 0644); err != nil {
		t.Fatal(err)
	}
	source := fmt.Sprintf(`package repair
import("context";"sdo.dev/operator/sdk/program")
type Program struct{}
func(Program) Spec()program.ProgramSpec{return program.ProgramSpec{ID:%q,Params:[]program.ParamSpec{{Name:"service",Kind:"Service",Required:true}}}}
func(Program) Diagnose(context.Context,program.ReadView,program.Bindings)(program.Verdict,error){return program.Verdict{Result:program.Match},nil}
func(Program) Mitigate(_ context.Context,v program.ReadView,b program.Bindings)(program.Plan,error){%s}
`, spec.ID, body)
	if err := os.WriteFile(filepath.Join(dir, "diagnose.go"), []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	workspace, binary, err := builder.BuildWorkingProgram(context.Background(), builder.Config{ApplicationRoot: root, OperatorRoot: ".."}, spec.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = workspace.Close() })
	return &workingProgram{spec: spec, binary: binary, canMitigate: true, workspace: workspace}
}

type transientCompiledReadView struct {
	program.ReadView
	reads atomic.Int32
}

func (v *transientCompiledReadView) Logs(sdk.ObjectRef, string, time.Duration) (string, error) {
	if v.reads.Add(1) == 1 {
		return "", fmt.Errorf("synthetic observation unavailable")
	}
	return "synthetic observation", nil
}
func TestCompiledProgramObservationFailureRemainsRetryable(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	originalView := loop.ReadView
	view := &transientCompiledReadView{}
	loop.ReadView = func(ctx sdk.DetectionContext) program.ReadView { view.ReadView = originalView(ctx); return view }
	compiled := compiledTerminalPlanProgram(t, `_,err:=v.Logs(b.Objects["service"],"synthetic",0);if err!=nil{return program.Plan{},err};return program.Plan{Actions:[]program.Action{program.SetField{Ref:b.Objects["service"],Path:"spec.ports[0].targetPort",Value:program.FromSource(b.Objects["service"],"spec.ports[0].targetPort"),Reason:"synthetic repair"}}},nil`)
	var calls atomic.Int32
	p := terminalShapeProgram{compiled: compiled, calls: &calls}
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
	if calls.Load() != 2 || view.reads.Load() != 2 || len(report.Mitigations) != 1 || report.Mitigations[0].Result != "applied_cleared" {
		t.Fatalf("transient compiled observation became terminal: calls=%d reads=%d report=%v", calls.Load(), view.reads.Load(), report)
	}
}
