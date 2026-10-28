package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// switchableSourceView answers source comparisons from live state the test
// changes between attempts, like a cluster that keeps moving after a closure.
type switchableSourceView struct {
	mu   sync.Mutex
	diff program.Diff
	err  error
}

func (v *switchableSourceView) set(diff program.Diff, err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.diff, v.err = diff, err
}

func (v *switchableSourceView) drift() DriftControllerConfig {
	return DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView {
		v.mu.Lock()
		defer v.mu.Unlock()
		return sourceCompareView{ReplayReadView: &sdktest.ReplayReadView{}, diff: v.diff, err: v.err}
	}}
}

var stillDrifted = program.Diff{Changed: true, Text: "accepted: {} observed: {\"spec\":{\"ports\":[{\"targetPort\":9999}]}}"}

// completeDriftFinding fills the drift finding of newSourceInformationController
// so the closure passes the broker's and the state store's validation.
func completeDriftFinding(controller *Controller) {
	drift := &controller.pendingClosure.Request.Findings[len(controller.pendingClosure.Request.Findings)-1]
	drift.RuleID, drift.Severity, drift.Summary, drift.Evidence = "unexplained", sdk.SeverityWarning, "targetPort differs from accepted source", "spec.ports[0].targetPort"
}

// CLOSE2: the first broker attempt receives the closure-time recheck's notes
// and stores the closure in its ledger, then fails before an outcome commits
// (the controller exits for a health refresh). Live drift changes before the
// retry, and the controller restarts from its durable state. The retry must
// hand the broker the identical closure; recomputing its evidence from later
// live state made every retry fail with "closure differs from persisted broker
// evidence" until the closure permanently failed.
func TestClosureIdentityIsFrozenAcrossBrokerRetryAndRestart(t *testing.T) {
	for _, later := range []struct {
		name string
		diff program.Diff
		err  error
	}{
		{"drift restored", program.Diff{}, nil},
		{"source not comparable", program.Diff{}, errors.New("dry-run: connection refused")},
	} {
		t.Run(later.name, func(t *testing.T) {
			ctx := context.Background()
			target := nativeRepository(t)
			repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			interrupted := true
			native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "test", ResponderModel: "test",
				Deriver: OutcomeDeriverFunc(func(_ context.Context, closure IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
					if interrupted {
						return DerivedOutcome{}, errors.New("controller exited for health refresh")
					}
					return testDerivedOutcome(closure.Request.IncidentID, "success", true), nil
				}),
				Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
					return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "already learned"}, nil
				}),
			}
			view := &switchableSourceView{diff: stillDrifted}
			now := time.Unix(1000, 0).UTC()
			first := newSourceInformationController(t, &windowDetector{confirmed: true}, view.drift(), native, &now)
			id := first.pendingClosure.Request.IncidentID
			workspace, err := native.PrepareIncident(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			first.pendingClosure.Request.RepositoryWorktree, first.pendingClosure.Request.RepositoryBaseCommit = workspace.Worktree, workspace.BaseCommit
			first.pendingClosure.Request.RepairPolicy = "recorded-actions"
			completeDriftFinding(first)

			executeRecheck(t, first)
			awaitBrokerResult(t, first.closureResults)
			first.processBrokerCompletions()
			ledger, err := native.readLedger(id)
			if err != nil || ledger.Closure == nil || ledger.OutcomeCommit != "" || first.closureFailure == nil {
				t.Fatalf("fixture must stop after the ledger stores the closure and before the outcome: ledger=%+v failure=%+v err=%v", ledger, first.closureFailure, err)
			}
			if !strings.Contains(strings.Join(ledger.Closure.GetSourceCheckNotes(), ";"), "unexplained drift remains on Service/front") {
				t.Fatalf("fixture: the first attempt must carry the recheck's drift note: %v", ledger.Closure.GetSourceCheckNotes())
			}
			durable := exportState(t, first)
			if !proto.Equal(durable.GetPendingClosure(), ledger.Closure) {
				t.Fatal("the closure the broker stored is not the controller's durable pending closure")
			}

			interrupted = false
			view.set(later.diff, later.err)
			restarted, err := NewController(testControllerConfig(),
				[]sdk.Detector{controllerDetector("fault", time.Hour, stateFinding("fault")), &windowDetector{confirmed: true}},
				staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now)
			if err != nil {
				t.Fatal(err)
			}
			restarted.now = func() time.Time { return now }
			restarted.ConfigureDrift(view.drift())
			if err := restarted.RestoreState(durable); err != nil {
				t.Fatal(err)
			}
			if err := restarted.SetIncidentBroker(native); err != nil {
				t.Fatal(err)
			}
			now = now.Add(restarted.config.ClosureRetry.MaxBackoff)
			executeRecheck(t, restarted)
			awaitBrokerResult(t, restarted.closureResults)
			restarted.processBrokerCompletions()
			if restarted.closureState != contracts.ClosureState_CLOSURE_STATE_COMMITTED || restarted.closureFailure != nil {
				t.Fatalf("retry after restart did not commit the same closure: state=%v failure=%+v", restarted.closureState, restarted.closureFailure)
			}
			ledger, err = native.readLedger(id)
			if err != nil || ledger.OutcomeCommit == "" || !proto.Equal(ledger.Closure, durable.GetPendingClosure()) {
				t.Fatalf("committed evidence differs from the first attempt's closure: %+v %v", ledger, err)
			}
		})
	}
}

// The class: every closure the broker receives is the controller's durable
// pending closure, for every recheck path that adds information to it.
func TestBrokerReceivesExactlyTheDurablePendingClosure(t *testing.T) {
	for _, tc := range []struct {
		name  string
		drift DriftControllerConfig
		wait  bool
	}{
		{"source difference", DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView {
			return sourceCompareView{ReplayReadView: &sdktest.ReplayReadView{}, diff: stillDrifted}
		}}, false},
		{"source cannot render", DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView {
			return sourceCompareView{ReplayReadView: &sdktest.ReplayReadView{}, err: errors.New("render failed")}
		}}, true},
		{"inventory not loaded", DriftControllerConfig{Inventory: true, View: func(sdk.DetectionContext) program.ReadView {
			return sourceCompareView{ReplayReadView: &sdktest.ReplayReadView{}}
		}}, true},
		{"no source notes", DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView {
			return sourceCompareView{ReplayReadView: &sdktest.ReplayReadView{}}
		}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1000, 0).UTC()
			broker := &recordingClosureBroker{}
			controller := newSourceInformationController(t, &windowDetector{confirmed: true}, tc.drift, broker, &now)
			executeRecheck(t, controller)
			if tc.wait {
				assertStillPending(t, controller, broker)
				now = now.Add(closureInventoryLoadBound)
				executeRecheck(t, controller)
			}
			awaitBrokerResult(t, controller.closureResults)
			controller.processBrokerCompletions()
			if len(broker.closures) != 1 {
				t.Fatalf("broker calls = %d, want 1", len(broker.closures))
			}
			sent, err := incidentClosureContract(broker.closures[0])
			if err != nil {
				t.Fatal(err)
			}
			if durable := exportState(t, controller).GetPendingClosure(); !proto.Equal(sent, durable) {
				t.Fatalf("broker received a closure the controller never persisted:\nsent    %v\ndurable %v", sent, durable)
			}
		})
	}
}

// The freeze is durable before the broker sees the closure: a failed state
// save hands the broker nothing, and what the broker later receives is the
// closure a restarted controller loads.
func TestFrozenClosureIsDurableBeforeBrokerAttempt(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	broker := &recordingClosureBroker{}
	controller := newSourceInformationController(t, &windowDetector{confirmed: true}, DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView {
		return sourceCompareView{ReplayReadView: &sdktest.ReplayReadView{}, diff: stillDrifted}
	}}, broker, &now)
	completeDriftFinding(controller)
	file := crashStateStore{path: filepath.Join(t.TempDir(), "state")}
	store := &retryFailingStateStore{StateStore: file}
	if err := controller.AttachStateStore(ctx, store); err != nil {
		t.Fatal(err)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	store.failures = 1
	effect, ok := controller.PendingClosureEffect()
	if !ok || !effect.Freeze {
		t.Fatalf("an unfrozen closure must be frozen first: %+v %v", effect, ok)
	}
	if err := controller.ExecuteClosureEffect(ctx, effect); err == nil {
		t.Fatal("a failed save of the frozen closure must stop the handoff")
	}
	if broker.processCalls() != 0 {
		t.Fatal("broker received a closure that was never saved")
	}
	for attempt := 0; broker.processCalls() == 0 && attempt < 3; attempt++ {
		effect, ok := controller.PendingClosureEffect()
		if !ok {
			t.Fatal("frozen closure has no pending effect")
		}
		if err := controller.ExecuteClosureEffect(ctx, effect); err != nil && !errors.Is(err, ErrEffectNotDurable) {
			t.Fatal(err)
		}
		if err := controller.PersistState(ctx); err != nil {
			t.Fatal(err)
		}
	}
	awaitBrokerResult(t, controller.closureResults)
	controller.processBrokerCompletions()
	loaded, _, err := file.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sent, err := incidentClosureContract(broker.closures[0])
	if err != nil {
		t.Fatal(err)
	}
	if !sent.GetFrozen() || !proto.Equal(sent, loaded.GetPendingClosure()) {
		t.Fatalf("broker closure is not the durable frozen closure:\nsent   %v\nloaded %v", sent, loaded.GetPendingClosure())
	}
}

// A closure that differs from the broker's stored frozen closure names the
// differing fields and is not retried: the same durable closure would fail
// the same comparison on every attempt.
func TestBrokerIdentityMismatchNamesFieldsAndIsNotRetried(t *testing.T) {
	ctx := context.Background()
	target := nativeRepository(t)
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "test", ResponderModel: "test",
		Deriver: OutcomeDeriverFunc(func(context.Context, IncidentClosure, string, string, string) (DerivedOutcome, error) {
			return DerivedOutcome{}, errors.New("controller exited for health refresh")
		})}
	workspace, err := native.PrepareIncident(ctx, "identity")
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Request.RepairPolicy = "recorded-actions"
	if _, err := native.ProcessClosure(ctx, closure); err == nil {
		t.Fatal("fixture must interrupt the first attempt")
	}
	changed := closure
	changed.SourceCheckNotes = []string{"recomputed from later live state"}
	_, err = native.ProcessClosure(ctx, changed)
	var mismatch *ClosureIdentityError
	if !errors.As(err, &mismatch) || !mismatch.NonRetryable() || strings.Join(mismatch.Fields, ",") != "source_check_notes" {
		t.Fatalf("mismatch = %v, want a non-retryable error naming source_check_notes", err)
	}
}
