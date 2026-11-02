package runtime

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk/program"
)

type additionRepairDispatcher struct {
	t      *testing.T
	client *planFakeClient
	source *unstructured.Unstructured
	called bool
}

func (d *additionRepairDispatcher) Dispatch(ctx context.Context, r IncidentRequest) (IncidentResult, error) {
	d.called = true
	if len(r.Findings) != 1 || r.Findings[0].DetectorID != "drift.unexplained" || len(r.DriftEvents) != 1 || len(r.DriftEvents[0].Fields) == 0 {
		d.t.Fatalf("stage brief lost producer evidence: %+v", r)
	}
	executor := PlanExecutor{Client: d.client, Source: planFakeSource{source: d.source}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	result, err := executor.Apply(ctx, r, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: r.Findings[0].PrimaryResource}}}, false)
	if err != nil || !result.Applied {
		d.t.Fatalf("stage repair: %+v %v", result, err)
	}
	return IncidentResult{}, nil
}
