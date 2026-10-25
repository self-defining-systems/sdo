package runtime

import (
	"context"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
)

func TestPendingDetectorStatusPersistsThroughControllerState(t *testing.T) {
	evaluations := []DetectorEvaluation{{DetectorID: "service-reachability", EvaluatedAt: time.Unix(100, 0), Status: DetectorEvaluationPending}}
	store := lifecycleio.NewConfigMapStateStore(fake.NewSimpleClientset(), "control", "sdo-controller-state")
	state := &contracts.ControllerState{Version: RuntimeStateVersion, History: evaluationsContract(evaluations)}
	if _, err := store.Save(context.Background(), state, ""); err != nil {
		t.Fatalf("persist unconfirmed detector: %v", err)
	}
	restored, _, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := evaluationsState(restored.History)
	if len(got) != 1 || got[0].Status != DetectorEvaluationPending {
		t.Fatalf("pending changed on restart: %+v", got)
	}
}
