package runtime

import (
	"context"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

type scopedIgnoringDetector struct{}

func (scopedIgnoringDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "scoped", Watches: []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "foreign"}}}
}

func (scopedIgnoringDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

type scopedIgnoringDiagnoser struct{}

func (scopedIgnoringDiagnoser) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "ignore-scoped-error"}
}

func (scopedIgnoringDiagnoser) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	_, _ = view.(sdk.ScopedObservationSource).ScopedDeployments("foreign")
	return program.Verdict{Result: program.NoMatch}, nil
}

func TestScopedWrappersKeepDenialUnknown(t *testing.T) {
	target := sdk.ObservationTarget{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "foreign"}
	scoped := sdk.ScopedSnapshot{Version: 1, Scope: sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}, Statuses: []sdk.ObservationStatus{{Target: target, State: "forbidden"}}}
	base := sdktest.Snapshot{NamespaceName: "app", ScopedSnapshot: scoped}
	live := LiveReadView{Carrier: inputs.New(base)}
	recording := &sdktest.RecordingReadView{ReadView: live}
	for _, view := range []sdk.DetectionContext{base, trafficContext{Carrier: inputs.New(base), ScopedObservationSource: base}, live, recording, struct{ sdk.DetectionContext }{base}} {
		if _, err := sdk.DetectObserved(context.Background(), scopedIgnoringDetector{}, view); err == nil {
			t.Fatalf("wrapper hid unavailable dependency: %T", view)
		}
	}
	verdict := program.RunDiagnose(context.Background(), scopedIgnoringDiagnoser{}, recording, program.Bindings{Objects: program.ObjectBindings{}}, time.Second)
	if verdict.Result != program.Unknown {
		t.Fatalf("ignored scoped error cleared diagnosis: %v", verdict)
	}
	replay := &sdktest.ReplayReadView{Snapshot: base, Calls: recording.Calls}
	verdict = program.RunDiagnose(context.Background(), scopedIgnoringDiagnoser{}, replay, program.Bindings{Objects: program.ObjectBindings{}}, time.Second)
	if verdict.Result != program.Unknown {
		t.Fatal("scoped failure did not replay")
	}
}

func TestScopedAggregateMissingFieldsCannotClear(t *testing.T) {
	diff := program.Diff{Changed: true, Text: `accepted: {"webhooks":[{"name":"before"}]}` + "\n" + `observed: {"webhooks":[{"name":"after"}]}`}
	if driftFieldsRestored(diff, []sdk.DriftEvent{{Fields: []sdk.DriftField{{Path: "webhooks"}}}}) {
		t.Fatal("two missing aggregate leaves established restoration")
	}
}

func TestScopedCompositionPreservesObservationClock(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	target := sdk.ObservationTarget{APIVersion: "v1", Kind: "Service", Namespace: "foreign"}
	scoped := sdk.ScopedSnapshot{Version: 1, Generation: "commit", Scope: sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}, Statuses: []sdk.ObservationStatus{{Target: target, State: sdk.ObservationObserved}}}
	base := sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &now, ScopedSnapshot: scoped}
	trafficView := trafficContext{Carrier: inputs.New(base), ScopedObservationSource: base}
	live := LiveReadView{Carrier: inputs.New(trafficView)}
	recorded := &sdktest.RecordingReadView{ReadView: live}
	for _, view := range []sdk.DetectionContext{trafficView, live, recorded} {
		if !sdk.ObservationTime(view).Equal(now) {
			t.Fatalf("%T hid observation clock", view)
		}
		capture, err := sdktest.CaptureSnapshot(view, "app")
		if err != nil || !sdk.ObservationTime(capture).Equal(now) || capture.ObservationGeneration() != "commit" {
			t.Fatalf("wrapper capture changed clock/scope: %T %v", view, err)
		}
	}
}

