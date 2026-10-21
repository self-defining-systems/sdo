package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type hostileMitigator struct {
	candidateProgram
	behavior string
	release  <-chan struct{}
	entered  chan<- struct{}
}

func (p hostileMitigator) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	if p.entered != nil {
		p.entered <- struct{}{}
	}
	if p.behavior == "panic" {
		panic("authored mitigator panic")
	}
	<-p.release
	return program.Plan{}, nil
}
func TestResponderLoopBoundsPanickingAndHangingMitigators(t *testing.T) {
	for _, behavior := range []string{"panic", "hang"} {
		t.Run(behavior, func(t *testing.T) {
			release := make(chan struct{})
			defer close(release)
			entered := make(chan struct{}, 1)
			p := hostileMitigator{candidateProgram: candidateProgram{spec: program.ProgramSpec{ID: "hostile"}, run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }}, behavior: behavior, release: release, entered: entered}
			registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
			if err != nil {
				t.Fatal(err)
			}
			loop := NewResponderLoop(IncidentRequest{IncidentID: "i", Namespace: "app"}, registry, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, nil, PlanExecutor{}, 1)
			loop.DiagnosisDeadline = 20 * time.Millisecond
			loop.ReadView = func(sdk.DetectionContext) program.ReadView { return &sdktest.ReplayReadView{} }
			started := time.Now()
			response := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "hostile"})
			if response.Error == "" || time.Since(started) > time.Second {
				t.Fatalf("mitigator escaped boundary: %+v", response)
			}
			if behavior == "panic" && !strings.Contains(response.Error, "panic") {
				t.Fatalf("lost panic context: %+v", response)
			}
			if len(loop.FailureSignals()) == 0 {
				t.Fatal("missing mitigator failure signal")
			}
		})
	}
}
func TestResponderLoopReleasesMutexDuringMitigation(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	entered := make(chan struct{}, 1)
	p := hostileMitigator{candidateProgram: candidateProgram{spec: program.ProgramSpec{ID: "hostile"}}, behavior: "hang", release: release, entered: entered}
	registry, _ := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	loop := NewResponderLoop(IncidentRequest{IncidentID: "i", Namespace: "app"}, registry, staticProvider{snapshot: sdktest.Snapshot{}}, nil, PlanExecutor{}, 1)
	loop.DiagnosisDeadline = 200 * time.Millisecond
	loop.ReadView = func(sdk.DetectionContext) program.ReadView { return &sdktest.ReplayReadView{} }
	done := make(chan struct{})
	go func() {
		defer close(done)
		loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "hostile"})
	}()
	<-entered
	read := make(chan struct{})
	go func() { loop.LoopsUsed(); close(read) }()
	select {
	case <-read:
	case <-time.After(50 * time.Millisecond):
		t.Error("mitigator holds loop mutex")
	}
	<-done
}

type responderTestSnapshotProvider struct {
	mu       sync.Mutex
	snapshot sdktest.Snapshot
}

func (p *responderTestSnapshotProvider) Set(snapshot sdktest.Snapshot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, _ := json.Marshal(snapshot)
	_ = json.Unmarshal(data, &p.snapshot)
}
func (p *responderTestSnapshotProvider) Snapshot(context.Context) (sdk.DetectionContext, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, err := json.Marshal(p.snapshot)
	if err != nil {
		return nil, err
	}
	var snapshot sdktest.Snapshot
	err = json.Unmarshal(data, &snapshot)
	return snapshot, err
}
