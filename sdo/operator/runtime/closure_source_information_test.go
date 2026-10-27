package runtime

import (
	"errors"
	"strings"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// sourceCompareView answers every source comparison with one fixed result.
type sourceCompareView struct {
	*sdktest.ReplayReadView
	diff program.Diff
	err  error
}

func (v sourceCompareView) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) {
	return v.diff, v.err
}

// newSourceInformationController returns a controller holding a pending closure
// that health verified and whose findings include a drift field on a Service.
func newSourceInformationController(t *testing.T, detector *windowDetector, drift DriftControllerConfig, broker IncidentBroker, now *time.Time) *Controller {
	t.Helper()
	controller := newClosureRecheckController(t, detector, broker, now)
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "front"}
	event := sdk.DriftEvent{Ref: ref, Fields: []sdk.DriftField{{Path: "spec.ports[0].targetPort"}}, Classification: sdk.DriftUnexplained}
	finding := sdk.Finding{DetectorID: "drift.unexplained", Status: sdk.FindingActive, Fingerprint: "drift", PrimaryResource: ref, Metadata: map[string]any{"drift_event": event}}
	controller.pendingClosure.Request.Findings = append(controller.pendingClosure.Request.Findings, finding)
	controller.ConfigureDrift(drift)
	return controller
}

// Source and render state are information only. A closure that health verified
// waits for its source checks within the bound, then commits as verified with
// the unavailable checks noted; it is neither retired nor downgraded.
func TestVerifiedClosureStaysSuccessWhenSourceChecksAreUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		drift DriftControllerConfig
		note  string
	}{
		{"source cannot render", DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView {
			return sourceCompareView{ReplayReadView: &sdktest.ReplayReadView{}, err: errors.New("normalize accepted manifest for Service/front: dry-run: connection refused")}
		}}, "were not compared"},
		{"inventory not loaded", DriftControllerConfig{Inventory: true, View: func(sdk.DetectionContext) program.ReadView {
			return sourceCompareView{ReplayReadView: &sdktest.ReplayReadView{}}
		}}, "inventory is loading"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1000, 0).UTC()
			broker := &recordingClosureBroker{}
			controller := newSourceInformationController(t, &windowDetector{confirmed: true}, tc.drift, broker, &now)

			executeRecheck(t, controller)
			assertStillPending(t, controller, broker)
			if controller.pendingClosure.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
				t.Fatalf("an unavailable source check made the closure unconfirmed before its bound: %+v", controller.pendingClosure)
			}
			now = now.Add(closureInventoryLoadBound)
			executeRecheck(t, controller)
			awaitBrokerResult(t, controller.closureResults)
			controller.processBrokerCompletions()
			if controller.closureState != contracts.ClosureState_CLOSURE_STATE_COMMITTED || len(broker.closures) != 1 {
				t.Fatalf("expiry did not commit the verified closure: state=%v failure=%+v closures=%d", controller.closureState, controller.closureFailure, len(broker.closures))
			}
			got := broker.closures[0]
			if len(got.FinalDetectorStates) != 1 || len(got.PartialDetectorStates) != 0 || got.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
				t.Fatalf("source state downgraded a closure health verified: %+v", got)
			}
			if len(got.SourceCheckNotes) == 0 || !strings.Contains(strings.Join(got.SourceCheckNotes, ";"), tc.note) {
				t.Fatalf("the unavailable source check is not noted for the outcome: %v", got.SourceCheckNotes)
			}
		})
	}
}

// A comparison that still shows the field changed is information too: the
// closure health verified commits, with the difference noted on the outcome.
func TestClosureWithObservedSourceDifferenceCommitsWithNote(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	broker := &recordingClosureBroker{}
	controller := newSourceInformationController(t, &windowDetector{confirmed: true}, DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView {
		return sourceCompareView{ReplayReadView: &sdktest.ReplayReadView{}, diff: program.Diff{Changed: true, Text: "accepted: {} observed: {\"spec\":{\"ports\":[{\"targetPort\":9999}]}}"}}
	}}, broker, &now)
	controller.OnClosureFailed = func(failure ClosureFailure) { t.Fatalf("a source difference retired a health-verified closure: %+v", failure) }

	executeRecheck(t, controller)
	awaitBrokerResult(t, controller.closureResults)
	controller.processBrokerCompletions()
	if controller.closureState != contracts.ClosureState_CLOSURE_STATE_COMMITTED || len(broker.closures) != 1 {
		t.Fatalf("closure did not commit: state=%v failure=%+v", controller.closureState, controller.closureFailure)
	}
	got := broker.closures[0]
	if len(got.FinalDetectorStates) != 1 || !strings.Contains(strings.Join(got.SourceCheckNotes, ";"), "unexplained drift remains on Service/front") {
		t.Fatalf("difference not recorded or closure downgraded: %+v", got)
	}
}

// Drift never rescues a closure whose health is not verified.
func TestObservedSourceDifferenceDoesNotRescueIncompleteHealth(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	broker := &recordingClosureBroker{}
	controller := newSourceInformationController(t, &windowDetector{}, DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView {
		return sourceCompareView{ReplayReadView: &sdktest.ReplayReadView{}, diff: program.Diff{Changed: true, Text: "accepted: {} observed: {\"spec\":{\"ports\":[{\"targetPort\":9999}]}}"}}
	}}, broker, &now)

	executeRecheck(t, controller)
	assertStillPending(t, controller, broker)
	now = now.Add(windowRecheckBound)
	executeRecheck(t, controller)
	if controller.pendingClosure.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVATION_INCOMPLETE {
		t.Fatalf("an incomplete health window must still commit unconfirmed: %+v", controller.pendingClosure)
	}
}

// Health that is itself incomplete still downgrades at the bound; source notes
// do not rescue it.
func TestSourceNotesDoNotRescueIncompleteHealth(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	broker := &recordingClosureBroker{}
	controller := newSourceInformationController(t, &windowDetector{}, DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView {
		return sourceCompareView{ReplayReadView: &sdktest.ReplayReadView{}, err: errors.New("render failed")}
	}}, broker, &now)

	executeRecheck(t, controller)
	assertStillPending(t, controller, broker)
	now = now.Add(windowRecheckBound)
	executeRecheck(t, controller)
	if controller.pendingClosure.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVATION_INCOMPLETE {
		t.Fatalf("an incomplete health window must still commit unconfirmed: %+v", controller.pendingClosure)
	}
}

// The outcome records what differs from the accepted source even when the
// closure has no final detector states to recheck or ended in a dispatch error.
func TestClosureWithoutRecheckableHealthStillRecordsSourceDifference(t *testing.T) {
	for _, tc := range []struct {
		name  string
		shape func(*IncidentClosure)
	}{
		{"no final detector states", func(c *IncidentClosure) { c.FinalDetectorStates = nil }},
		{"dispatch error", func(c *IncidentClosure) { c.DispatchError = "responder job failed" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1000, 0).UTC()
			broker := &recordingClosureBroker{}
			controller := newSourceInformationController(t, &windowDetector{confirmed: true}, DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView {
				return sourceCompareView{ReplayReadView: &sdktest.ReplayReadView{}, diff: program.Diff{Changed: true, Text: "accepted: {} observed: {\"spec\":{\"ports\":[{\"targetPort\":9999}]}}"}}
			}}, broker, &now)
			tc.shape(controller.pendingClosure)

			executeRecheck(t, controller)
			awaitBrokerResult(t, controller.closureResults)
			controller.processBrokerCompletions()
			if len(broker.closures) != 1 || !strings.Contains(strings.Join(broker.closures[0].SourceCheckNotes, ";"), "unexplained drift remains on Service/front") {
				t.Fatalf("difference not recorded on the outcome: %+v", broker.closures)
			}
		})
	}
}
