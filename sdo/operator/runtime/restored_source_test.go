package runtime

import (
	"context"
	"sync/atomic"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// scriptedDiffView answers DiffAgainstSource from diffs, one per call in
// order, counting calls.
type scriptedDiffView struct {
	program.ReadView
	diffs []program.Diff
	calls *atomic.Int32
}

func (v scriptedDiffView) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) {
	n := int(v.calls.Add(1)) - 1
	if n >= len(v.diffs) {
		return v.diffs[len(v.diffs)-1], nil
	}
	return v.diffs[n], nil
}

type staticSnapshots struct{ view sdktest.Snapshot }

func (s staticSnapshots) Snapshot(context.Context) (sdk.DetectionContext, error) { return s.view, nil }

func restoreLoop(request IncidentRequest, calls *atomic.Int32, diffs ...program.Diff) *ResponderLoop {
	loop := &ResponderLoop{Request: request, Provider: staticSnapshots{view: sdktest.Snapshot{NamespaceName: "app"}}}
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return scriptedDiffView{ReadView: &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}, diffs: diffs, calls: calls}
	}
	return loop
}

// A write's receipt proves it restored accepted source only when the object
// departed from source when the incident opened (an opening drift finding or
// the configuration diff at dispatch), no earlier SDO write touched it, and it
// matches source after the write. Secret writes are never diffed.
func TestRestoredSourceNeedsADepartureTheIncidentOpenedOn(t *testing.T) {
	config := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "settings"}
	drift := sdk.Finding{DetectorID: "drift.source", RuleID: "changed", Status: sdk.FindingActive, PrimaryResource: config}
	opened := IncidentRequest{Namespace: "app", Findings: []sdk.Finding{drift}}
	reconcile := program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: config}}}
	departed, clean := program.Diff{Changed: true}, program.Diff{}
	for name, test := range map[string]struct {
		request IncidentRequest
		plan    program.Plan
		earlier bool
		diffs   []program.Diff
		want    bool
	}{
		"departure at open, restored":        {opened, reconcile, false, []program.Diff{departed, clean}, true},
		"departure at open, still departed":  {opened, reconcile, false, []program.Diff{departed, departed}, false},
		"no departure before the write":      {opened, reconcile, false, []program.Diff{clean, clean}, false},
		"departure not seen at open":         {IncidentRequest{Namespace: "app"}, reconcile, false, []program.Diff{departed, clean}, false},
		"an earlier SDO write touched it":    {opened, reconcile, true, []program.Diff{departed, clean}, false},
		"source-absent object deleted":       {opened, reconcile, false, []program.Diff{{Changed: true, SourceAbsent: true}, {SourceAbsent: true}}, true},
		"source-absent object still present": {opened, reconcile, false, []program.Diff{{Changed: true, SourceAbsent: true}, {Changed: true, SourceAbsent: true}}, false},
		"configuration diff at dispatch": {IncidentRequest{Namespace: "app", StateChanges: &StateChanges{Changes: []StateChange{{Kind: "ConfigMap", Namespace: "app", Name: "settings", Change: "modified"}}}},
			reconcile, false, []program.Diff{departed, clean}, true},
	} {
		t.Run(name, func(t *testing.T) {
			calls := &atomic.Int32{}
			loop := restoreLoop(test.request, calls, test.diffs...)
			if test.earlier {
				loop.applicationsLog = []RecordedPlanApplication{{ID: "earlier", Result: PlanApplicationResult{ObjectEffects: []PlanObjectEffect{{Operation: "patch", Ref: config, State: "applied"}}}}}
			}
			before := loop.sourceRestoreBefore(context.Background(), test.plan)
			if got := before.restored(context.Background(), loop); got != test.want {
				t.Fatalf("restored = %v, want %v", got, test.want)
			}
		})
	}
	// A Secret write never reaches the diff path and never restores source.
	calls := &atomic.Int32{}
	secret := sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "credentials"}
	loop := restoreLoop(IncidentRequest{Namespace: "app", Findings: []sdk.Finding{{DetectorID: "drift.source", PrimaryResource: secret}}}, calls, departed, clean)
	before := loop.sourceRestoreBefore(context.Background(), program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: secret}}})
	if before.restored(context.Background(), loop) || calls.Load() != 0 {
		t.Fatalf("a Secret write was diffed (%d calls) or credited", calls.Load())
	}
}
