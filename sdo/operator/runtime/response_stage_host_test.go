package runtime

import (
	"context"
	"testing"

	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/runtime/sim"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// hostedSeeds keeps host round trips to a few seeds: each runs gRPC over bufconn.
func hostedSeeds(t *testing.T, body func(*testing.T, *sim.World)) {
	t.Helper()
	for seed := uint64(1); seed <= 8; seed++ {
		sim.Run(t, seed, body)
	}
}

// hostedStage moves the sim fast-path loop's detectors and programs behind an
// in-process memory host over bufconn, so every diagnosis, plan and detector
// evaluation the stage needs crosses MemoryHostService.
type hostedStage struct {
	loop    *ResponderLoop
	kube    *sim.Kube
	manager *memoryhost.Manager
	remote  *RemoteMemory
	stop    func()
	world   *sim.World
}

func newHostedStage(t *testing.T, w *sim.World) *hostedStage {
	t.Helper()
	ctx := context.Background()
	loop, kube := simFastPathLoop(t, w)
	registry, err := program.NewRegistry([]program.Diagnoser{fastloopPortProgram{}}, []program.Mitigator{fastloopPortProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	server, err := memoryhost.NewServer("accepted", "sha256:test", []sdk.Detector{fastloopDetector{}})
	if err != nil {
		t.Fatal(err)
	}
	server.WithPrograms(registry)
	rpc, stop := memoryTestRPC(t, server)
	manager := memoryhost.NewManager(0)
	t.Cleanup(func() { _ = manager.Close() })
	snapshot, err := w.Cache.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Publish(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	host := &simActivationHost{testMemoryRPC: rpc, stop: stop}
	if err := manager.Prepare(ctx, host); err != nil {
		t.Fatal(err)
	}
	if err := manager.Activate(ctx, host, "accepted", "sha256:test"); err != nil {
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
	loop.Programs, loop.Detectors = remote.Programs, remote.Detectors
	loop.Trust = nil
	return &hostedStage{loop: loop, kube: kube, manager: manager, remote: remote, stop: stop, world: w}
}

func (h *hostedStage) run(t *testing.T) *IncidentState {
	t.Helper()
	state := stageState(h.loop)
	stage := &DeterministicStage{NewLoop: func(IncidentRequest) *ResponderLoop { return h.loop }}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage}); err != nil {
		t.Fatal(err)
	}
	return state
}

func (h *hostedStage) servicePort(t *testing.T) any {
	t.Helper()
	object, err := h.kube.Get(context.Background(), sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"})
	if err != nil {
		t.Fatal(err)
	}
	port, _ := testField(object, "spec.ports.0.targetPort")
	return port
}

// failed asserts the stage wrote nothing and claimed nothing: the fault stays
// open, nothing is healthy, and no attempt reads as applied or cleared.
func (h *hostedStage) failed(t *testing.T, state *IncidentState, writes int) {
	t.Helper()
	if len(state.Findings) == 0 || state.Healthy {
		t.Fatalf("host failure cleared the incident: findings=%v healthy=%v", state.Findings, state.Healthy)
	}
	for _, attempt := range state.PriorStages[0].GetMitigations() {
		if attempt.GetResult() == "applied_cleared" {
			t.Fatalf("attempt claims clearance: %v", attempt)
		}
	}
	if got := len(h.loop.PlanApplications()); got != writes {
		t.Fatalf("applications = %d, want %d", got, writes)
	}
}

func TestDeterministicStageClearsThroughMemoryHost(t *testing.T) {
	hostedSeeds(t, func(t *testing.T, w *sim.World) {
		h := newHostedStage(t, w)
		state := h.run(t)
		if len(state.Findings) != 0 || !state.Healthy {
			t.Fatalf("not cleared: %v %v", state.Findings, state.PriorStages)
		}
		if attempt := state.PriorStages[0].GetMitigations()[0]; attempt.GetResult() != "applied_cleared" {
			t.Fatalf("attempt = %v", attempt)
		}
		if h.servicePort(t) == int64(9999) {
			t.Fatal("service still faulted")
		}
	})
}

// A host that is gone before the stage starts is a failed attempt: no write,
// no clearance, no authorized empty plan.
func TestDeterministicStageHostDownBeforeStageWritesNothing(t *testing.T) {
	hostedSeeds(t, func(t *testing.T, w *sim.World) {
		h := newHostedStage(t, w)
		h.stop()
		state := h.run(t)
		h.failed(t, state, 0)
		if h.servicePort(t) != int64(9999) {
			t.Fatal("stage wrote without the host")
		}
		if attempt := state.PriorStages[0].GetMitigations()[0]; attempt.GetSkipReason() != "diagnose_unavailable" {
			t.Fatalf("attempt = %v", attempt)
		}
	})
}

// The host dies when the stage's write is already in flight: the write is
// recorded and never replayed, and the missing observation cannot clear it.
func TestDeterministicStageHostCrashDuringWriteIsNotClearance(t *testing.T) {
	hostedSeeds(t, func(t *testing.T, w *sim.World) {
		h := newHostedStage(t, w)
		before := h.kube.Before
		h.kube.Before = func(ctx context.Context, edge string) (sim.Fault, error) {
			if edge == "patch" {
				h.stop()
			}
			return before(ctx, edge)
		}
		state := h.run(t)
		if len(state.Findings) == 0 || state.Healthy {
			t.Fatalf("crash cleared the incident: %v %v", state.Findings, state.Healthy)
		}
		for _, attempt := range state.PriorStages[0].GetMitigations() {
			if attempt.GetResult() == "applied_cleared" {
				t.Fatalf("attempt claims clearance: %v", attempt)
			}
		}
		if h.loop.stageWrites != 1 {
			t.Fatalf("write count = %d", h.loop.stageWrites)
		}
	})
}

// A swap to a new accepted revision between the open-time diagnosis and the
// stage retires the adapter this incident was dispatched with. Its programs
// error rather than run against code that was not accepted for the incident.
func TestDeterministicStageRetiredRevisionWritesNothing(t *testing.T) {
	hostedSeeds(t, func(t *testing.T, w *sim.World) {
		h := newHostedStage(t, w)
		ctx := context.Background()
		server, err := memoryhost.NewServer("next", "sha256:next", []sdk.Detector{fastloopDetector{}})
		if err != nil {
			t.Fatal(err)
		}
		registry, err := program.NewRegistry([]program.Diagnoser{fastloopPortProgram{}}, []program.Mitigator{fastloopPortProgram{}})
		if err != nil {
			t.Fatal(err)
		}
		server.WithPrograms(registry)
		rpc, stop := memoryTestRPC(t, server)
		next := &simActivationHost{testMemoryRPC: rpc, stop: stop}
		if err := h.manager.Prepare(ctx, next); err != nil {
			t.Fatal(err)
		}
		if err := h.manager.Activate(ctx, next, "next", "sha256:next"); err != nil {
			t.Fatal(err)
		}
		state := h.run(t)
		h.failed(t, state, 0)
		if h.servicePort(t) != int64(9999) {
			t.Fatal("stage wrote through a retired revision")
		}
	})
}
