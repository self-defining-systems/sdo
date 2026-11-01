package runtime

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	repositorybroker "sdo.dev/operator/broker"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/runtime/sim"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type simActivationHost struct {
	testMemoryRPC
	stop func()
}

func (h *simActivationHost) Ready(ctx context.Context) (*pb.ReadyResponse, error) {
	return h.client.Ready(ctx, &pb.ReadyRequest{})
}
func (h *simActivationHost) Close() error { h.stop(); return nil }
func (h *simActivationHost) MirrorFrame(ctx context.Context, frame *pb.MirrorRequest) error {
	stream, err := h.client.Mirror(ctx)
	if err != nil {
		return err
	}
	if err = stream.Send(frame); err != nil {
		return err
	}
	ack, err := stream.CloseAndRecv()
	if err == nil && ack.Sequence != frame.Sequence {
		return errors.New("mirror acknowledgment mismatch")
	}
	return err
}

type revisionProgram struct{ revision string }

func (p revisionProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "revision"} }
func (p revisionProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.Match}, nil
}
func (p revisionProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Rationale: p.revision, Actions: []program.Action{program.RestartRollout{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "toy", Name: p.revision}}}}, nil
}
func activationHost(t *testing.T, commit string, ds []sdk.Detector) *simActivationHost {
	s, err := memoryhost.NewServer(commit, "digest", ds)
	if err != nil {
		t.Fatal(err)
	}
	p := revisionProgram{commit}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	s.WithPrograms(registry)
	rpc, stop := memoryTestRPC(t, s)
	return &simActivationHost{testMemoryRPC: rpc, stop: stop}
}
func TestSimMemoryTwoAcceptedCommits(t *testing.T) {
	sim.Run(t, 43, func(t *testing.T, w *sim.World) {
		ctx := context.Background()
		manager := memoryhost.NewManager(time.Second)
		defer manager.Close()
		snapshot := sdktest.Snapshot{NamespaceName: "toy"}
		if err := manager.Publish(ctx, snapshot); err != nil {
			t.Fatal(err)
		}
		ds := []sdk.Detector{scheduledDetector{sdk.DetectorSpec{ID: "original", Interval: time.Second}}}
		initial := activationHost(t, "initial", ds)
		if err := manager.Prepare(ctx, initial); err != nil {
			t.Fatal(err)
		}
		if err := manager.Activate(ctx, initial, "initial", "digest"); err != nil {
			t.Fatal(err)
		}
		ready, _ := initial.Ready(ctx)
		remote, err := NewRemoteMemory(ready, manager, func(sdk.DetectionContext) (uint64, error) { return 0, nil })
		if err != nil {
			t.Fatal(err)
		}
		config := testControllerConfig()
		config.Namespace = "toy"
		provider := staticProvider{snapshot: snapshot}
		controller, err := NewController(config, remote.Detectors, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, w.Now())
		if err != nil {
			t.Fatal(err)
		}
		identity := controller
		controller.tracker.states["retained"] = &FindingState{DetectorID: "original", Active: true, FiringCount: 3, ClearCount: 1}
		retained := controller.tracker.states["retained"]
		a := &memoryActivation{manager: manager, active: "initial", results: make(chan memoryCandidate, 1), notify: make(chan struct{}, 1)}
		fail := true
		releaseBuild := make(chan struct{})
		a.build = func(ctx context.Context, commit string) memoryCandidate {
			<-releaseBuild
			if fail {
				return memoryCandidate{commit: commit, err: errors.New("synthetic build failure")}
			}
			next := append([]sdk.Detector{}, ds...)
			next = append(next, scheduledDetector{sdk.DetectorSpec{ID: "added", Interval: time.Second}})
			return memoryCandidate{host: activationHost(t, commit, next), commit: commit, digest: "digest", close: func() {}}
		}
		a.install = func(ctx context.Context, candidate memoryCandidate) error {
			ready, err := candidate.host.Ready(ctx)
			if err != nil {
				return err
			}
			next, err := NewRemoteMemory(ready, manager, func(sdk.DetectionContext) (uint64, error) { return 0, nil })
			if err != nil {
				return err
			}
			if err = manager.Activate(ctx, candidate.host, candidate.commit, candidate.digest); err != nil {
				return err
			}
			remote = next
			return controller.ReconfigureMemory(next, provider, w.Now())
		}
		// A failed accepted candidate must keep detection on the old revision.
		if ok, err := a.refresh(ctx, "add-detector"); ok || err != nil {
			t.Fatal(ok, err)
		}
		if _, err := remote.Detectors[0].Detect(ctx, snapshot); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		w.Sleep(ctx, 50*time.Millisecond)
		close(releaseBuild)
		synctest.Wait()
		if ok, err := a.refresh(ctx, "add-detector"); ok || err == nil {
			t.Fatal("build failure was hidden")
		}
		fail = false
		for _, commit := range []string{"add-detector", "change-program"} {
			releaseBuild = make(chan struct{})
			previous := remote
			start := w.Now()
			if ok, err := a.refresh(ctx, commit); ok || err != nil {
				t.Fatal(ok, err)
			}
			for i := 0; i < 3; i++ {
				if _, err := remote.Detectors[0].Detect(ctx, snapshot); err != nil {
					t.Fatal("detection stopped during compilation", err)
				}
			}
			w.Sleep(ctx, 50*time.Millisecond)
			close(releaseBuild)
			synctest.Wait()
			if ok, err := a.refresh(ctx, commit); !ok || err != nil {
				t.Fatal(ok, err)
			}
			if controller != identity || len(controller.detectors) != 2 || controller.tracker.states["retained"] != retained || retained.ClearCount != 1 {
				t.Fatal("activation lost controller state")
			}
			oldProgram, _ := previous.Programs.Mitigator("revision")
			if _, err := oldProgram.Mitigate(ctx, &sdktest.ReplayReadView{Snapshot: snapshot}, nil); err == nil {
				t.Fatal("retired revision reused trust against new code")
			}
			mitigator, _ := remote.Programs.Mitigator("revision")
			plan, err := mitigator.Mitigate(ctx, &sdktest.ReplayReadView{Snapshot: snapshot}, nil)
			if err != nil || plan.Rationale != commit {
				t.Fatal("stale program", plan, err)
			}
			t.Logf("virtual build-to-activation latency for %s: %s", commit, w.Now().Sub(start))
		}

		manager.SetRecovery(func(recoverCtx context.Context) (memoryhost.Host, string, string, uint64, error) {
			sameRevision := append([]sdk.Detector{}, ds...)
			sameRevision = append(sameRevision, scheduledDetector{sdk.DetectorSpec{ID: "added", Interval: time.Second}})
			host := activationHost(t, "change-program", sameRevision)
			if err := manager.Prepare(recoverCtx, host); err != nil {
				return nil, "", "", 0, err
			}
			ready, err := host.Ready(recoverCtx)
			return host, "change-program", "digest", ready.GetMirrorSequence(), err
		})
		_ = a.current.host.Close()
		if _, err := remote.Detectors[0].Detect(ctx, snapshot); err == nil {
			t.Fatal("host crash cleared detector")
		}
		synctest.Wait()
		identityAfterCrash, err := manager.Identity(ctx)
		if err != nil || identityAfterCrash.GetMemoryCommit() != "change-program" || len(identityAfterCrash.GetDetectors()) != 2 {
			t.Fatal("same-revision recovery failed", identityAfterCrash, err)
		}
		if _, err := remote.Detectors[0].Detect(ctx, snapshot); err != nil {
			t.Fatal("recovered detector unavailable", err)
		}
	})
}

func TestHostActivationGatesAcknowledgment(t *testing.T) {
	sim.Run(t, 8, func(t *testing.T, w *sim.World) {
		c, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, w.Now())
		if err != nil {
			t.Fatal(err)
		}
		receipt := ClosureReceipt{IncidentID: "host-ack", OutcomeCommit: "outcome", ReflectionCommit: "memory", AckToken: "token"}
		c.closureState = pb.ClosureState_CLOSURE_STATE_COMMITTED
		c.closureReceipt = &receipt
		broker := &rolloutRecordingBroker{pending: true}
		if err = c.SetIncidentBroker(broker); err != nil {
			t.Fatal(err)
		}
		native := NativeIncidentBroker{Repository: &repositorybroker.Broker{}, MemoryHost: true, LedgerRoot: t.TempDir(), Now: w.Now}
		expected := ControllerRolloutExpectation{IncidentID: receipt.IncidentID, ReflectionCommit: "memory", AfterDetectorFingerprint: "tree"}
		if err = native.saveLedger(&pb.BrokerLedger{IncidentId: receipt.IncidentID, ReflectionCommit: "memory", RolloutExpectation: rolloutExpectationContract(&expected), Receipt: closureReceiptContract(&receipt)}); err != nil {
			t.Fatal(err)
		}
		activated := false
		c.memoryRefresh = func(context.Context) (bool, error) { return activated, nil }
		effect, ok := c.PendingClosureAcknowledgmentEffect()
		if !ok {
			t.Fatal("ACK not pending")
		}
		if err = c.ExecuteClosureAcknowledgmentEffect(context.Background(), effect); err != nil {
			t.Fatal(err)
		}
		if len(broker.calls()) != 0 {
			t.Fatal("stale memory acknowledged closure")
		}
		bad := ControllerRolloutReceipt{Expectation: expected, BuildArtifact: "host-digest", ControllerRevision: "stale", ObservedDetectorFingerprint: "tree", Success: true, VerifiedAt: w.Now()}
		if err = native.RecordControllerRollout(bad); err == nil {
			t.Fatal("stale activation accepted")
		}
		bad.ControllerRevision = "memory"
		if err = native.RecordControllerRollout(bad); err != nil {
			t.Fatal(err)
		}
		recovered := NativeIncidentBroker{Repository: &repositorybroker.Broker{}, LedgerRoot: native.LedgerRoot, MemoryHost: true}
		if _, pending, err := recovered.PendingControllerRollout("memory"); err != nil || pending {
			t.Fatal("activation was not durable", pending, err)
		}
		activated = true
		broker.pending = false
		if err = c.ExecuteClosureAcknowledgmentEffect(context.Background(), effect); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if len(broker.calls()) != 1 {
			t.Fatal("activated memory did not release ACK")
		}
	})
}

func TestRemoteMemoryRemovesUsageDeclaration(t *testing.T) {
	host, _ := memoryhost.NewServer("c", "d", []sdk.Detector{scheduledDetector{sdk.DetectorSpec{ID: "no-usage", Interval: time.Second}}})
	ready, _ := host.Ready(context.Background(), nil)
	remote, err := NewRemoteMemory(ready, remoteFailure{}, func(sdk.DetectionContext) (uint64, error) { return 0, nil })
	if err != nil {
		t.Fatal(err)
	}
	base := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "toy"}}
	if _, collects := withUsage(base, remote.Detectors, nil, nil).(*usageSnapshotProvider); collects {
		t.Fatal("remote detector without a usage declaration still collects kubelet inputs")
	}
}

func TestActivationSupersededDuringBuildStartsLatest(t *testing.T) {
	sim.Run(t, 17, func(t *testing.T, w *sim.World) {
		disposed := false
		built := make(chan string, 1)
		a := &memoryActivation{active: "initial", pending: "superseded", results: make(chan memoryCandidate, 1), notify: make(chan struct{}, 1)}
		a.results <- memoryCandidate{commit: "superseded", close: func() { disposed = true }}
		a.build = func(_ context.Context, commit string) memoryCandidate {
			built <- commit
			return memoryCandidate{commit: commit, err: errors.New("synthetic unavailable candidate")}
		}
		defer a.close()
		if ready, err := a.refresh(context.Background(), "latest"); ready || err != nil {
			t.Fatal(ready, err)
		}
		synctest.Wait()
		select {
		case commit := <-built:
			if commit != "latest" {
				t.Fatal(commit)
			}
		default:
			t.Fatal("new accepted revision waits for an unrelated wakeup")
		}
		if !disposed || a.active != "initial" {
			t.Fatal("superseded candidate became active")
		}
	})
}

func TestCandidatePreparationIsBounded(t *testing.T) {
	sim.Run(t, 19, func(t *testing.T, w *sim.World) {
		ctx := context.Background()
		a := &memoryActivation{active: "initial", results: make(chan memoryCandidate, 1), notify: make(chan struct{}, 1)}
		a.build = func(ctx context.Context, commit string) memoryCandidate {
			<-ctx.Done()
			return memoryCandidate{commit: commit, err: ctx.Err()}
		}
		defer a.close()
		if ready, err := a.refresh(ctx, "next"); ready || err != nil {
			t.Fatal(ready, err)
		}
		w.Sleep(ctx, 10*time.Minute)
		synctest.Wait()
		if ready, err := a.refresh(ctx, "next"); ready || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("candidate readiness is unbounded", ready, err)
		}
		if a.active != "initial" {
			t.Fatal("timed-out candidate replaced active memory")
		}
	})
}
