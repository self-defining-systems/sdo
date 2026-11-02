package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
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
	// The handoff steps the closure to PROCESSING synchronously; the broker call
	// itself runs on a background goroutine, so its count is not yet observable.
	for attempt := 0; controller.closureState != contracts.ClosureState_CLOSURE_STATE_PROCESSING && attempt < 3; attempt++ {
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
	if controller.closureState != contracts.ClosureState_CLOSURE_STATE_PROCESSING {
		t.Fatalf("closure never reached the broker: state %v", controller.closureState)
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
