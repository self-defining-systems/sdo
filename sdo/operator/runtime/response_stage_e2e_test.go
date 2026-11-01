package runtime

import (
	"context"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/runtime/sim"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// scriptedAgent stands in for the responder: it repairs the cluster the way an
// agent would (a direct write) and reports nothing the controller must trust.
type scriptedAgent struct {
	kube  *sim.Kube
	world *sim.World
	runs  int
	prior []*contracts.StageReport
}

func (a *scriptedAgent) Name() string { return "agent" }
func (a *scriptedAgent) Run(_ context.Context, state *IncidentState) (*contracts.StageReport, error) {
	a.runs++
	a.prior = cloneStageReports(state.PriorStages)
	a.kube.Put(simService(8080))
	a.world.Cache.CatchUp()
	return nil, nil
}

// activateHost starts a memory host for one accepted revision, with or without
// the saved program, and returns the controller-side adapter for it.
func activateHost(t *testing.T, ctx context.Context, manager *memoryhost.Manager, commit string, withProgram bool) *RemoteMemory {
	t.Helper()
	server, err := memoryhost.NewServer(commit, "sha256:"+commit, []sdk.Detector{fastloopDetector{}})
	if err != nil {
		t.Fatal(err)
	}
	if withProgram {
		registry, err := program.NewRegistry([]program.Diagnoser{fastloopPortProgram{}}, []program.Mitigator{fastloopPortProgram{}})
		if err != nil {
			t.Fatal(err)
		}
		server.WithPrograms(registry)
	}
	rpc, stop := memoryTestRPC(t, server)
	host := &simActivationHost{testMemoryRPC: rpc, stop: stop}
	if err := manager.Prepare(ctx, host); err != nil {
		t.Fatal(err)
	}
	if err := manager.Activate(ctx, host, commit, "sha256:"+commit); err != nil {
		t.Fatal(err)
	}
	ready, err := host.Ready(ctx)
	if err != nil {
		t.Fatal(err)
	}
	remote, err := NewRemoteMemory(ready, manager)
	if err != nil {
		t.Fatal(err)
	}
	return remote
}

// First incident: no saved program, so the agent repairs it. A later accepted
// revision saves the program; the repeat incident is cleared by the stage with
// no agent run and no responder claim. A swap between the open-time diagnosis
// and the stage of a third incident makes the stage fail closed to the agent.
func TestDeterministicStageRepeatClearsWithoutResponderAfterSavedProgram(t *testing.T) {
	hostedSeeds(t, func(t *testing.T, w *sim.World) {
		ctx := context.Background()
		first, kube := simFastPathLoop(t, w)
		snapshot, err := w.Cache.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		manager := memoryhost.NewManager(0)
		t.Cleanup(func() { _ = manager.Close() })
		if err := manager.Publish(ctx, snapshot); err != nil {
			t.Fatal(err)
		}
		bare := activateHost(t, ctx, manager, "before-learning", false)
		agent := &scriptedAgent{kube: kube, world: w}
		incident := func(loop *ResponderLoop, remote *RemoteMemory) (*IncidentState, error) {
			loop.Programs, loop.Detectors, loop.Trust = remote.Programs, remote.Detectors, nil
			state := stageState(loop)
			stage := &DeterministicStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}
			return state, RunResponseStages(ctx, state, []ResponseStage{stage, agent})
		}

		// Incident 1: nothing matches, so the agent runs and repairs.
		state, err := incident(first, bare)
		if err != nil || agent.runs != 1 {
			t.Fatalf("first incident: agent runs=%d err=%v", agent.runs, err)
		}
		if len(agent.prior) != 1 || len(agent.prior[0].GetMitigations()) != 1 || agent.prior[0].GetMitigations()[0].GetSkipReason() != "no_mitigation" {
			t.Fatalf("agent was not told the stage had no mitigation: %v", agent.prior)
		}
		if len(first.PlanApplications()) != 0 || state.Result != nil {
			t.Fatal("stage wrote without a saved program")
		}

		// Reflection saves a program: a new accepted revision activates.
		learned := activateHost(t, ctx, manager, "after-learning", true)

		// Incident 2: the same fault returns; the stage clears it alone.
		second, _ := simFastPathLoop(t, w)
		second.Request.IncidentID = "repeat"
		kube.Put(simService(9999))
		w.Cache.CatchUp()
		second.Request.Findings = nil
		view, err := w.Cache.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		findings, err := (fastloopDetector{}).Detect(ctx, view)
		if err != nil || len(findings) != 1 {
			t.Fatalf("repeat must start faulted: %v %v", findings, err)
		}
		second.Request.Findings, second.findings = findings, findings
		second.Programs, second.Detectors = learned.Programs, learned.Detectors
		second.InitialDiagnose(ctx)
		runsBefore := agent.runs
		state, err = incident(second, learned)
		if err != nil {
			t.Fatal(err)
		}
		if agent.runs != runsBefore || len(state.Findings) != 0 || !state.Healthy || state.Result != nil {
			t.Fatalf("repeat reached the agent: runs=%d findings=%v healthy=%v", agent.runs, state.Findings, state.Healthy)
		}
		if got := state.PriorStages[0].GetMitigations()[0].GetResult(); got != "applied_cleared" || len(second.PlanApplications()) != 1 {
			t.Fatalf("repeat result %q applications=%d", got, len(second.PlanApplications()))
		}

		// Incident 3: a newer revision activates after the open-time diagnosis,
		// retiring the adapter this incident holds. The stage writes nothing and
		// the agent gets the incident with the reason recorded.
		third, _ := simFastPathLoop(t, w)
		kube.Put(simService(9999))
		w.Cache.CatchUp()
		view, _ = w.Cache.Snapshot(ctx)
		findings, _ = (fastloopDetector{}).Detect(ctx, view)
		third.Request.Findings, third.findings = findings, findings
		third.Programs, third.Detectors = learned.Programs, learned.Detectors
		third.InitialDiagnose(ctx)
		activateHost(t, ctx, manager, "swapped", true)
		runsBefore = agent.runs
		state, err = incident(third, learned)
		if err != nil || agent.runs != runsBefore+1 {
			t.Fatalf("swap did not hand off to the agent: runs=%d err=%v", agent.runs, err)
		}
		if len(third.PlanApplications()) != 0 {
			t.Fatal("stage wrote through a retired revision")
		}
		if attempt := agent.prior[0].GetMitigations(); len(attempt) != 1 || attempt[0].GetResult() == "applied_cleared" {
			t.Fatalf("agent prior = %v", agent.prior)
		}
	})
}
