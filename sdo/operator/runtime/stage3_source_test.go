package runtime

import (
	"context"
	"errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"testing"
	"time"
)

// Direct apply uses the ordinary bounded convergence path. At its deadline,
// old rollout children remain pending, while real source drift stays partial.
func TestSourceRolloutResidualAtBoundRemainsTypedPending(t *testing.T) {
	ref := addedFieldsRef()
	rollouts := &stage3SourceRollout{pending: true}
	executor := PlanExecutor{Client: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: addedFieldsLive()}}, Source: planFakeSource{source: addedFieldsSource()}, Rollouts: rollouts, Verifier: planFakeVerifier{passed: true}, RolloutTimeout: 20 * time.Millisecond}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, reconcilePlan(ref), false)
	var pending *RolloutPendingError
	if !errors.As(err, &pending) || result.RolloutPending == nil || result.ActionReports[0].Status != "applied" {
		t.Fatalf("rollout observation became failure: result=%+v err=%v", result, err)
	}
	receipt, err := result.Proto()
	if err != nil {
		t.Fatal(err)
	}
	restored := planApplicationState(receipt).Result
	if restored.RolloutPending == nil {
		t.Fatal("durable receipt lost pending")
	}
	rollouts.pending = false
	if err := executor.reportSourceResiduals(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted"}, &restored); err != nil || !restored.ActionReports[0].SourceReconcile.Converged {
		t.Fatalf("later readback failed to converge: %+v %v", restored, err)
	}
}

type stage3SourceRollout struct{ pending bool }

func (*stage3SourceRollout) Wait(context.Context, []sdk.ObjectRef) error { return nil }
func (r *stage3SourceRollout) SourceRolloutResiduals(context.Context, *unstructured.Unstructured) ([]string, error) {
	if r.pending {
		return []string{"rollout.pods[app/old-revision]"}, nil
	}
	return nil, nil
}
