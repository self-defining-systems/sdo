package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/sdktest"
)

func TestFastPathResultUsesExecutorClock(t *testing.T) {
	started := time.Unix(123, 0).UTC()
	completed := started.Add(7 * time.Second)
	loop := &ResponderLoop{Executor: PlanExecutor{Now: func() time.Time { return completed }}}
	result := fastPathResult(IncidentRequest{IncidentID: "clock"}, loop, FastPathAttempt{}, started)
	if !result.Timing.CompletedAt.Equal(completed) || !result.VerificationEvidence[0].ObservedAt.Equal(completed) || !result.Timing.StartedAt.Equal(started) {
		t.Fatalf("result timing %+v evidence %+v", result.Timing, result.VerificationEvidence)
	}
}

func TestRetainedDriftUsesEvaluationTime(t *testing.T) {
	at := time.Unix(456, 0).UTC()
	view := driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app"}}}
	unresolved, states, _ := checkRetainedDrift(context.Background(), []sdk.Finding{{DetectorID: "drift.unexplained"}}, view, nil, time.Second, nil, at)
	if len(unresolved) != 0 || len(states) != 1 || !states[0].EvaluatedAt.Equal(at) {
		t.Fatalf("states %+v, unresolved %+v", states, unresolved)
	}
}

type clockBaseline struct{ at time.Time }

func (*clockBaseline) Observe(time.Time, bool)              {}
func (b *clockBaseline) Changes(at time.Time) *StateChanges { b.at = at; return &StateChanges{} }
func TestLiveReadViewBaselineUsesSnapshotTime(t *testing.T) {
	at := time.Unix(789, 0).UTC()
	baseline := &clockBaseline{}
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &at}), BaselineSource: baseline}
	_, err := view.DiffAgainstBaseline(sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"})
	if err != nil || !baseline.at.Equal(at) {
		t.Fatalf("baseline time %s, error %v", baseline.at, err)
	}
}
