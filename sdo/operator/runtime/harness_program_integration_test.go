//go:build integration

package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
)

// Saved programs are agent-written code, an untrusted input to a controller
// that must stay reliable. Each detector of a plan has a stand-in program in
// the memory host whose Diagnose and Mitigate behave as the seed chooses,
// for the whole run (an agent fixes the application at quiesce, not the saved
// program).

// programBehaviour is what a stand-in program does when the host calls it.
type programBehaviour string

const (
	// programRight diagnoses the fault object correctly and plans the fix.
	programRight programBehaviour = "right"
	// programWrong diagnoses the opposite verdict and plans a write to a
	// different object than the fault.
	programWrong programBehaviour = "wrong"
	// programGarbage answers a verdict or plan that is not valid.
	programGarbage programBehaviour = "garbage"
	programError   programBehaviour = "error"
	programPanic   programBehaviour = "panic"
	programHang    programBehaviour = "hang"
)

// planProgram is the behaviour of one detector's program.
type planProgram struct {
	Diagnose, Mitigate programBehaviour
}

func programID(detector string) string { return "fix-" + detector }

// standinProgram is the saved program for one detector's fault object.
type standinProgram struct {
	h        *harness
	detector string
	other    string // another fault object, the target of a wrong plan
	modes    planProgram
}

func (p standinProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: programID(p.detector), Params: []program.ParamSpec{
		{Name: "fault", Kind: "ConfigMap", Required: true},
		{Name: "fixed", ValueKind: "string", Default: "false"},
		{Name: "broken", ValueKind: "string", Default: "true"},
	}}
}

func (p standinProgram) misbehave(ctx context.Context, verb string, behaviour programBehaviour) error {
	p.h.programCalls.Add(verb+"/"+string(behaviour), 1)
	switch behaviour {
	case programError:
		return errors.New("stand-in program error")
	case programPanic:
		panic("stand-in program panic")
	case programHang:
		select {
		case <-ctx.Done():
		case <-time.After(time.Hour):
		}
		return ctx.Err()
	}
	return nil
}

func (p standinProgram) Diagnose(ctx context.Context, view program.ReadView, b program.Bindings) (program.Verdict, error) {
	if err := p.misbehave(ctx, "diagnose", p.modes.Diagnose); err != nil {
		return program.Verdict{Result: program.Unknown}, err
	}
	if p.modes.Diagnose == programGarbage {
		return program.Verdict{Result: "garbage"}, nil
	}
	broken := false
	for _, cm := range view.ConfigMaps() {
		if cm.Name == b.Objects["fault"].Name && cm.Data["broken"] == "true" {
			broken = true
		}
	}
	if p.modes.Diagnose == programWrong {
		broken = !broken
	}
	evidence := []program.Evidence{{Fact: "fault object " + b.Objects["fault"].Name, Observed: fmt.Sprint(broken), Expected: "false"}}
	if broken {
		return program.Verdict{Result: program.Match, Evidence: evidence}, nil
	}
	return program.Verdict{Result: program.NoMatch, Evidence: evidence}, nil
}

func (p standinProgram) Mitigate(ctx context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	if err := p.misbehave(ctx, "mitigate", p.modes.Mitigate); err != nil {
		return program.Plan{}, err
	}
	ref := b.Objects["fault"]
	switch p.modes.Mitigate {
	case programGarbage:
		return program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "data.broken"}}}, nil
	case programWrong:
		other := ref
		other.Name = faultConfigMapName(p.other)
		return program.Plan{Actions: []program.Action{program.SetField{Ref: other, Path: "data[broken]", Value: program.FromParam("broken"), Reason: "wrong plan"}}}, nil
	}
	return program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "data[broken]", Value: program.FromParam("fixed"), Reason: "clear the fault"}}}, nil
}

// programsOf is the program registry a memory revision's host serves.
func (h *harness) programsOf(rev planRevision) (program.ProgramRegistry, error) {
	var diagnosers []program.Diagnoser
	var mitigators []program.Mitigator
	for i, d := range rev.Detectors {
		other := rev.Detectors[(i+1)%len(rev.Detectors)].ID
		p := standinProgram{h: h, detector: d.ID, other: other, modes: h.plan.Programs[d.ID]}
		if p.modes.Diagnose == "" {
			p.modes = planProgram{programRight, programRight}
		}
		diagnosers = append(diagnosers, p)
		mitigators = append(mitigators, p)
	}
	return program.NewRegistry(diagnosers, mitigators)
}

// DeterministicResponseStage lets the controller run its automatic stage over
// the saved programs, as the Job dispatcher does, before the agent is asked.
func (a harnessAgent) DeterministicResponseStage() ResponseStage {
	stage := &DeterministicResponseStage{NewLoop: func(request IncidentRequest) *ResponderLoop { return a.h.responderLoop(a.inc, request) }}
	return tapStage{stage, a.h}
}

// tapStage logs what the automatic stage did with the programs.
type tapStage struct {
	ResponseStage
	h *harness
}

func (s tapStage) Run(ctx context.Context, state *IncidentState) (*contracts.StageReport, error) {
	report, err := s.ResponseStage.Run(ctx, state)
	for _, m := range report.GetMitigations() {
		s.h.programCalls.Add("stage/"+m.GetResult()+"/"+m.GetSkipReason(), 1)
		if m.GetError() != "" {
			s.h.t.Logf("stage %s: %s", m.GetPlaybookId(), m.GetError())
		}
	}
	return report, err
}

// counter counts named events from any goroutine.
type counter struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *counter) Add(key string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[key] += n
}

func (c *counter) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]string, 0, len(c.n))
	for k := range c.n {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, " %s=%d", k, c.n[k])
	}
	return b.String()
}

// responderLoop is the loop the automatic stage runs: the controller's own
// programs (served by the memory host), snapshot provider and plan executor,
// writing through the faulty API with the controller process's identity.
func (h *harness) responderLoop(inc *incarnation, request IncidentRequest) *ResponderLoop {
	h.mu.Lock()
	c, remote := h.ctl, h.remote
	h.mu.Unlock()
	if c == nil || remote == nil {
		return nil
	}
	client := h.clientFor("sdo-controller", inc, true)
	executor := PlanExecutor{
		Client:   TypedPlanClient{Client: client},
		Source:   rendered.Source{Repository: h.repo, Git: harnessPlanGit},
		Now:      time.Now,
		Sleep:    func(ctx context.Context, d time.Duration) error { return sleepContext(ctx, d) },
		Rollouts: &planFakeRollout{},
		Verifier: PlanHealthVerifier{Detectors: remote.Detectors, Provider: c.provider, Timeout: 20 * time.Second, PollInterval: time.Second},
	}
	loop := NewResponderLoop(request, remote.Programs, c.provider, remote.Detectors, executor, 5)
	source := rendered.Source{Repository: h.repo, Git: harnessPlanGit}
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return LiveReadView{Carrier: inputs.New(snapshot), Context: context.Background(), Client: client, Source: source, SourceCommit: harnessAcceptedCommit}
	}
	loop.GitRead = harnessPlanGit
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	return loop
}

func sleepContext(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
