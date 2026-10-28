package runtime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"

	"google.golang.org/protobuf/proto"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
)

func simMemoryStructureValidator(ctx context.Context, path string, _ []string) error {
	diagnostics, err := validate.Validate(ctx, validate.Request{Root: path, Layers: validate.Structural})
	if err != nil {
		return err
	}
	return validate.Err(diagnostics)
}

// Shared schedule events operate on the simulated API rather than inserting
// detector findings. The controller remains responsible for classification.
type simScheduleEventKind string

const (
	simPause                 simScheduleEventKind = "maintenance-pause"
	simResume                simScheduleEventKind = "maintenance-resume"
	simForeignPodCreate      simScheduleEventKind = "foreign-pod-create"
	simForeignPodDelete      simScheduleEventKind = "foreign-pod-delete"
	simRestartFailingTraffic simScheduleEventKind = "restart-failing-traffic"
	simRestartChangedScope   simScheduleEventKind = "restart-changed-closure-scope"
)

type simScheduleEvent struct {
	tick int
	kind simScheduleEventKind
}

var simForeignPodRef = sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "foreign-probe"}

func (w *simWorld) applyScheduleEvent(event simScheduleEvent) {
	switch event.kind {
	case simPause:
		w.paused = true
	case simResume:
		w.paused = false
		if err := w.controller.ResumeObservation(context.Background(), w.clock.Now()); err != nil {
			w.t.Fatal(err)
		}
	case simRestartChangedScope:
		w.restart()
		w.controller.provider = simChangedScopeProvider{SnapshotProvider: w.controller.provider, generation: fmt.Sprintf("scope-after-restart-%d", w.restarts)}
	case simForeignPodCreate:
		pod := inventoryObject("v1", "Pod", "app", simForeignPodRef.Name)
		pod.SetUID("foreign-probe-uid")
		pod.SetResourceVersion("1")
		w.kube.objects[simForeignPodRef] = pod
	case simForeignPodDelete:
		delete(w.kube.objects, simForeignPodRef)
	default:
		w.t.Fatalf("unknown simulation event %q", event.kind)
	}
}

type simInventory struct{ world *simWorld }

func (s simInventory) SourceDriftObjects() []*unstructured.Unstructured {
	objects := make([]*unstructured.Unstructured, 0, len(s.world.kube.objects))
	for _, object := range s.world.kube.objects {
		objects = append(objects, object.DeepCopy())
	}
	return objects
}
func (simInventory) TakeObjectChanges() []ObjectChange { return nil }

// A foreign Pod is ordinary source-absent latent drift. It is information: an
// incident opened for another fault closes on health while the Pod exists, and
// the finding stays observable across maintenance/restart until it disappears.
func runSimForeignPodDuringUnrelatedIncident(t *testing.T, seed uint64) {
	t.Helper()
	w := newSimWorld(t, seed)
	w.client.ReactionChain = w.client.ReactionChain[2:]
	w.responder.behavior = func() simBehavior { return simFixNone }
	w.broker.reject = func() bool { return false }
	declared := make([]*unstructured.Unstructured, 0, simStreamServices)
	for _, object := range w.kube.objects {
		declared = append(declared, object.DeepCopy())
	}
	w.configure = func(c *Controller) {
		c.ConfigureDrift(DriftControllerConfig{Inventory: true, Events: simInventory{w}, Source: &inventorySource{objects: declared}, BatchWindow: time.Nanosecond})
	}
	w.restart()
	create := 4 + w.rng.IntN(5)
	remove := create + 15 + w.rng.IntN(5)
	events := []simScheduleEvent{{create, simForeignPodCreate}, {create + 3, simPause}, {remove - 2, simResume}, {remove, simForeignPodDelete}}
	seen := false
	// Closure follows removal within a few ticks plus the success soak.
	settle := 3 + int(w.controller.config.ClosureStabilityWindow/time.Second)
	for tick := 0; tick < remove+settle+27; tick++ {
		w.clock.now = w.clock.now.Add(time.Second)
		if tick == 1 {
			w.setPort(simServiceName(0), 9999)
		}
		for _, event := range events {
			if event.tick == tick {
				w.applyScheduleEvent(event)
			}
		}
		if tick == create+1 {
			w.setPort(simServiceName(0), 8080)
		}
		if tick == remove-1 {
			w.restart()
		}
		w.step()
		w.drain()
		if tick >= create && tick < remove {
			for _, finding := range w.controller.sourceInventoryFindings {
				if finding.PrimaryResource != simForeignPodRef {
					continue
				}
				event := inventoryEvent(finding)
				if finding.DetectorID != "drift.unexplained" || finding.Severity != sdk.SeverityLatent || event.Classification != sdk.DriftUnexplained || event.Source != "absent from accepted source" {
					t.Fatalf("foreign Pod misclassified: %+v", finding)
				}
				seen = true
			}
		}
		if tick >= remove+settle && (len(w.closed) != 1 || !w.idle()) {
			t.Fatalf("foreign Pod stranded unrelated closure: closures=%d open=%v", len(w.closed), w.controller.IncidentOpen())
		}
	}
	if !seen || len(w.closed) != 1 || len(w.closed[0].FinalDetectorStates) == 0 || w.responder.calls != 1 {
		t.Fatalf("foreign Pod schedule did not settle one verified incident: seen=%v closures=%+v calls=%d", seen, w.closed, w.responder.calls)
	}
	stored, err := w.outcomes.Get(context.Background(), w.closed[0].Request.IncidentID)
	if err != nil || stored.Record.GetIncidentId() == "" {
		t.Fatalf("closed incident has no durable outcome: %v", err)
	}
	if len(w.violations) > 0 {
		t.Fatal(w.violations)
	}
}

// A scripted worker repairs through the simulated API but keeps its report
// open. StopResponder represents the independently observed worker retirement;
// one seeded termination retry ensures paused wakeups cover that edge too.
type simReportingResponder struct {
	base        *simResponder
	repaired    chan IncidentResult
	release     chan struct{}
	stops       int
	terminating int
}

func (r *simReportingResponder) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	result, err := r.base.Dispatch(ctx, request)
	if err != nil {
		return result, err
	}
	result.ResponderSessionID = "sim-live-responder"
	r.repaired <- result
	select {
	case <-ctx.Done():
		return IncidentResult{}, ctx.Err()
	case <-r.release:
		return result, nil
	}
}
func (r *simReportingResponder) StopResponder(context.Context, string, int) error {
	r.stops++
	if r.stops <= r.terminating {
		return errResponderTerminating
	}
	return nil
}

// The scope changes independently of the API objects and persisted closure.
type simChangedScopeProvider struct {
	SnapshotProvider
	generation string
}
type simChangedScopeSnapshot struct {
	sdk.DetectionContext
	sdk.ScopedObservationSource
}

func (p simChangedScopeProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	snapshot, err := p.SnapshotProvider.Snapshot(ctx)
	return simChangedScopeSnapshot{DetectionContext: snapshot, ScopedObservationSource: sdk.ScopedSnapshot{Version: 1, Generation: p.generation}}, err
}

func runSimPausedClosurePublishesOutcomeAndReflection(t *testing.T, seed uint64) {
	t.Helper()
	restart := seed%2 == 0
	t.Run(fmt.Sprintf("restart=%t", restart), func(t *testing.T) {
		ctx := context.Background()
		w := newSimWorld(t, seed)
		w.client.ReactionChain = w.client.ReactionChain[2:]
		w.responder.behavior = func() simBehavior { return simFixAll }
		root := nativeRepository(t)
		nativeMemoryFixture(t, root)
		repository, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), simMemoryStructureValidator)
		if err != nil {
			t.Fatal(err)
		}
		var reflections atomic.Int32
		var interrupted atomic.Bool
		native := NativeIncidentBroker{Now: w.clock.Now, Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "scripted", ResponderModel: "none",
			Reflector: ReflectionWorkerFunc(func(_ context.Context, closure IncidentClosure, _ IncidentWorkspace, outcome *contracts.OutcomeRecord, _, outcomeCommit string, _ ReflectionFeedback) (ReflectionDecision, error) {
				if closure.Result.ResponderSessionID != "sim-live-responder" || outcome.GetIncidentId() != closure.Request.IncidentID || outcomeCommit == "" {
					return ReflectionDecision{}, fmt.Errorf("reflection lost closure/session/outcome binding")
				}
				if interrupted.CompareAndSwap(false, true) {
					return ReflectionDecision{}, errors.New("simulated validator transport interruption")
				}
				reflections.Add(1)
				// The schedule has no executable playbook author, so the shared
				// check rejects every attempt for the recorded hand repair. The
				// rejection must still end reflection durably and exactly once.
				return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "scripted schedule has no executable playbook author"}, nil
			})}
		reporter := &simReportingResponder{base: w.responder, repaired: make(chan IncidentResult, 1), release: make(chan struct{}), terminating: int((seed >> 1) % 2)}
		w.config.SourceCommit = nativeGit(t, root, "rev-parse", "HEAD")
		w.config.DeployedCommit = w.config.SourceCommit
		w.config.RepositoryWorktree = root
		w.brokerOverride = native
		w.configure = func(c *Controller) { c.dispatcher = reporter }
		w.restart()
		w.setPort(simServiceName(0), 9999)
		w.clock.now = w.clock.now.Add(time.Second)
		w.step()
		workspace, ok := w.controller.PendingWorkspaceEffect()
		if !ok {
			t.Fatal("workspace not scheduled")
		}
		pauseBeforeLaunch := func() {
			observations := w.cache.snapshots
			w.applyScheduleEvent(simScheduleEvent{kind: simPause})
			if err := executeStartupEffects(ctx, w.controller, true); err != nil {
				t.Fatal(err)
			}
			if w.responder.calls != 0 || w.cache.snapshots != observations {
				t.Fatal("maintenance evaluated the app or launched pending responder work")
			}
			w.applyScheduleEvent(simScheduleEvent{kind: simResume})
		}
		if seed%2 != 0 {
			pauseBeforeLaunch()
		}
		if err := w.controller.ExecuteWorkspaceEffect(ctx, workspace); err != nil {
			t.Fatal(err)
		}
		w.await(func() bool { return len(w.controller.workspaceResults) > 0 })
		if seed%2 == 0 {
			pauseBeforeLaunch()
		}
		w.step()
		dispatch, ok := w.controller.PendingDispatchEffect()
		if !ok {
			t.Fatal("responder not scheduled")
		}
		if err := w.controller.ExecuteDispatchEffect(ctx, dispatch); err != nil {
			t.Fatal(err)
		}
		active := w.controller
		t.Cleanup(func() {
			if active.responderCancel != nil {
				active.responderCancel()
			}
		})
		w.await(func() bool { return len(reporter.repaired) > 0 })
		result := <-reporter.repaired
		loop := NewResponderLoop(dispatch.Request, nil, w.cache, nil, PlanExecutor{}, 3)
		loop.menuSession = &contracts.MitigationSession{ProviderSessionId: result.ResponderSessionID}
		loop.menuRepairs = result.RepairActions
		w.controller.responderLoop = loop
		for tick := 0; tick < 40 && w.controller.IncidentOpen(); tick++ {
			w.clock.now = w.clock.now.Add(time.Second)
			w.step()
		}
		closure, ok := w.controller.PendingIncidentClosure()
		if !ok || !closure.VerifiedWhileResponderRunning {
			t.Fatal("scripted worker never reached early closure")
		}
		// The live failure paused exactly one second after verification. The
		// second pause position is seeded within the remaining report grace.
		offset := time.Second
		if !restart {
			offset = time.Duration(1+w.rng.IntN(28)) * time.Second
		}
		w.clock.now = closure.VerifiedAt.Add(offset)
		if seed%2 == 1 {
			if err := w.controller.EvaluateAll(ctx, w.clock.Now()); err != nil {
				t.Fatal(err)
			}
		}
		observations := w.cache.snapshots
		w.applyScheduleEvent(simScheduleEvent{kind: simPause})
		if restart {
			active.responderCancel()
			w.await(func() bool { return len(active.results) > 0 })
			w.restart()
		}
		if seed%2 == 1 {
			// An ACTIVE save may fail immediately before maintenance starts.
			// An unchanged paused step must restore known durability too.
			failInherited := true
			w.client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, kruntime.Object, error) {
				if !failInherited {
					return false, nil, nil
				}
				failInherited = false
				return true, nil, errors.New("simulated pre-pause state write failure")
			})
			if err := w.controller.PersistState(ctx); err == nil {
				t.Fatal("inherited state failure was not injected")
			}
		}
		if err := executePausedEffects(ctx, w.controller); err != nil {
			t.Fatal(err)
		}
		if w.controller.durable == nil {
			t.Fatal("unchanged paused step stranded an inherited state-save failure")
		}
		if _, ok := w.controller.PendingClosureEffect(); ok {
			t.Fatal("paused closure bypassed report grace")
		}
		if w.cache.snapshots != observations {
			t.Fatal("paused startup/report grace sampled the application")
		}
		deadline := closure.VerifiedAt.Add(w.controller.config.ResponderReportGrace)
		if wake := w.controller.PausedWake(); !wake.Equal(deadline) {
			t.Fatalf("paused report wake=%v, want %v", wake, deadline)
		}
		w.clock.now = deadline
		failedSave := (seed>>1)%2 == 1
		failNext := failedSave
		w.client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, kruntime.Object, error) {
			if !failNext {
				return false, nil, nil
			}
			failNext = false
			return true, nil, errors.New("simulated paused state write failure")
		})
		if err := executePausedEffects(ctx, w.controller); err != nil {
			t.Fatal(err)
		}
		if failedSave && len(w.controller.closureResults) > 0 {
			t.Fatal("broker ran before paused closure state persisted")
		}
		for retry := 0; retry < 2; retry++ {
			wake := w.controller.PausedWake()
			if wake.IsZero() {
				break
			}
			if !wake.Equal(w.clock.Now().Add(time.Second)) {
				t.Fatalf("paused retirement/save retry wake=%v", wake)
			}
			w.clock.now = wake
			if err := executePausedEffects(ctx, w.controller); err != nil {
				t.Fatal(err)
			}
		}
		w.await(func() bool { return len(w.controller.closureResults) > 0 })
		// Crash after the native broker durably commits its outcome, before a
		// receipt exists. Restart with a new observation scope, as after policy
		// adoption while a reflection validator is interrupted.
		ledgerBefore, err := native.readLedger(closure.Request.IncidentID)
		if err != nil || ledgerBefore.OutcomeCommit == "" || ledgerBefore.Receipt != nil {
			t.Fatalf("missing interrupted outcome checkpoint: %v", err)
		}
		if ledgerBefore.ReflectionProcessed || ledgerBefore.ReflectionCommit != "" {
			t.Fatal("interrupted reflection received learning credit")
		}
		if seed%2 == 1 {
			// The accepted Git commit survives a crash just before its ledger
			// marker is written. Restore the last pre-commit checkpoint; the
			// real broker must recover the existing attributed outcome.
			checkpoint := &contracts.BrokerLedger{
				IncidentId:         ledgerBefore.IncidentId,
				Closure:            proto.Clone(ledgerBefore.Closure).(*contracts.IncidentClosure),
				OutcomeDocument:    proto.Clone(ledgerBefore.OutcomeDocument).(*contracts.OutcomeRecord),
				ProposalCommit:     ledgerBefore.ProposalCommit,
				RequiresReflection: ledgerBefore.RequiresReflection,
			}
			if err := native.saveLedger(checkpoint); err != nil {
				t.Fatal(err)
			}
		}
		frozen, ok := w.controller.PendingIncidentClosure()
		if !ok {
			t.Fatal("interrupted closure disappeared")
		}
		if ok, err := native.committedOutcomeClosure(ctx, frozen, nil); err != nil || !ok {
			t.Fatalf("accepted outcome not authenticated: %v", err)
		}
		changed := *cloneIncidentClosure(&frozen)
		changed.DispatchError = "different frozen evidence"
		if ok, err := native.committedOutcomeClosure(ctx, changed, nil); err != nil || ok {
			t.Fatalf("different closure authenticated against accepted outcome: %v", err)
		}
		w.applyScheduleEvent(simScheduleEvent{kind: simRestartChangedScope})
		if err := executeStartupEffects(ctx, w.controller, true); err != nil {
			t.Fatalf("persisted superseded closure crashed restart: %v", err)
		}
		w.await(func() bool { return len(w.controller.closureResults) > 0 })
		if err := executePausedEffects(ctx, w.controller); err != nil {
			t.Fatal(err)
		}
		w.await(func() bool { return len(w.controller.acknowledgmentResults) > 0 })
		if err := executePausedEffects(ctx, w.controller); err != nil {
			t.Fatal(err)
		}
		id := closure.Request.IncidentID
		outcome, err := memory.OpenReader(root).Get(ctx, id)
		if err != nil || outcome.Record.GetIncidentId() != id {
			t.Fatalf("paused closure has no committed outcome: %v", err)
		}
		ledger, err := native.readLedger(id)
		attempts := reflections.Load()
		if err != nil || !ledger.ReflectionProcessed || !ledger.ReflectionRejected || attempts == 0 || int(attempts) != len(ledger.ReflectionRejections) || ledger.AcknowledgedAt == nil {
			t.Fatalf("paused closure stranded reflection/ack: reflections=%d rejections=%d acknowledged=%v err=%v", attempts, len(ledger.GetReflectionRejections()), ledger.GetAcknowledged(), err)
		}
		for _, rejection := range ledger.ReflectionRejections {
			if ds := rejection.GetDiagnostics(); len(ds) != 1 || ds[0].GetRule() != validate.RuleReflectionProgramRequired || !strings.Contains(ds[0].GetMessage(), `"repair"`) {
				t.Fatalf("reflection rejection does not name the uncovered hand repair: %v", ds)
			}
		}
		if ledger.OutcomeCommit != ledgerBefore.OutcomeCommit || ledger.OutcomeDocument.GetClassification() != ledgerBefore.OutcomeDocument.GetClassification() || ledger.ReflectionCommit != "" {
			t.Fatal("scope restart rewrote historical outcome or credited interrupted learning")
		}
		if reporter.stops != reporter.terminating+1 || w.controller.LastAcknowledgedIncidentID() != id {
			t.Fatal("report grace did not retire worker and acknowledge closure")
		}
		w.controller.provider = w.controller.provider.(simChangedScopeProvider).SnapshotProvider
		w.applyScheduleEvent(simScheduleEvent{kind: simResume})
		w.step()
		w.drain()
		if !w.idle() || reflections.Load() != attempts || len(w.violations) > 0 {
			t.Fatalf("resume duplicated or stranded closure: %v", w.violations)
		}
	})
}
