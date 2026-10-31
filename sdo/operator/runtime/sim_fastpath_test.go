package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/runtime/sim"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func simService(targetPort int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Service",
		"metadata": map[string]any{"namespace": "app", "name": "api"},
		"spec":     map[string]any{"ports": []any{map[string]any{"port": int64(80), "targetPort": targetPort}}},
	}}
}

// simFastPathLoop builds a trusted fast-path loop over a simulated API server
// and a stale-prone informer cache. Reads go through the cache; writes go to the server.
func simFastPathLoop(t *testing.T, w *sim.World) (*ResponderLoop, *sim.Kube) {
	t.Helper()
	faults := sim.DrawFaults(w.RNG)
	faults.FailAfterCommit = func() bool { return false } // transport faults are the plan sweep's concern
	kube, cache := w.Kube, w.Cache
	kube.SetFaults(faults)
	cache.SetFaults(faults)
	kube.Put(simService(9999))
	cache.CatchUp()
	view, err := cache.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	findings, err := (fastloopDetector{}).Detect(context.Background(), view)
	if err != nil || len(findings) != 1 {
		t.Fatalf("fixture must start faulted: %v %v", findings, err)
	}
	registry, err := program.NewRegistry([]program.Diagnoser{fastloopPortProgram{}}, []program.Mitigator{fastloopPortProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	clock := w
	loop := NewResponderLoop(IncidentRequest{IncidentID: "sim", Namespace: "app", SourceCommit: "tree-a", RepairPolicy: "recorded-actions", Findings: findings},
		registry, cache, []sdk.Detector{fastloopDetector{}}, PlanExecutor{
			Client: kube, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}, Observer: simWriteObserver{cache}, Now: clock.Now, Sleep: clock.Sleep,
		}, 5)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	loop.DiagnosisSettleDelay = time.Millisecond
	loop.CorpusReplayPassed = true
	diagnosed := false
	w.Invariant(func(event sim.Event) error {
		if event.Kind == "mutation" && !diagnosed {
			return fmt.Errorf("mutation without diagnosis")
		}
		return nil
	})
	before := kube.Before
	kube.Before = func(ctx context.Context, edge string) (sim.Fault, error) {
		if edge == "patch" {
			if err := w.Emit("mutation", sdk.ObjectRef{}); err != nil {
				return sim.Fault{}, err
			}
		}
		return before(ctx, edge)
	}
	loop.InitialDiagnose(context.Background())
	diagnosed = true
	loop.Trust = map[string]ProgramTrust{"service-targetport-mismatch": {VerifiedSuccesses: 3, LastChange: "tree-a", Trusted: true}}
	return loop, kube
}

// TestSimTrustedFastPathNeverRollsBackAVerifiedFix: the cluster is repaired by
// a correct plan, so no read lag of the informer cache may make the fast path
// undo it (live incident 6: rolled back 14.5 ms after a correct write).
func TestSimTrustedFastPathNeverRollsBackACorrectFix(t *testing.T) {
	sim.Sweep(t, func(t *testing.T, w *sim.World) {
		loop, kube := simFastPathLoop(t, w)
		attempt := loop.TryTrustedFastPath(context.Background(), 3)
		object, _ := kube.Get(context.Background(), sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"})
		port, _ := testField(object, "spec.ports.0.targetPort")
		if attempt.RolledBack || attempt.RollbackErr != "" {
			t.Fatalf("correct fix rolled back (live port %v): %s", port, attempt.Summary)
		}
		if !attempt.Resolved {
			t.Fatalf("correct fix not resolved: %s", attempt.Summary)
		}
	})
}

type simWriteObserver struct{ *sim.Cache }

func (c simWriteObserver) AwaitWrites(context.Context, []ObservedWrite) error {
	c.CatchUp()
	return nil
}
