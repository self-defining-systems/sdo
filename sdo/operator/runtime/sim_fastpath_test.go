package runtime

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func simService(targetPort int64) *unstructured.Unstructured {
	return simServiceNamed("api", targetPort)
}

func simServiceNamed(name string, targetPort int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Service",
		"metadata": map[string]any{"namespace": "app", "name": name},
		"spec":     map[string]any{"ports": []any{map[string]any{"port": int64(80), "targetPort": targetPort}}},
	}}
}

// simFastPathLoop builds a trusted fast-path loop over a simulated API server
// and a stale-prone informer cache. Reads go through the cache; writes go to the server.
func simFastPathLoop(t *testing.T, seed uint64) (*ResponderLoop, *simKube) {
	t.Helper()
	rng := newSimRNG(seed)
	faults := drawSimFaults(rng)
	// Ambiguous transport faults are the plan sweep's concern; read lag and
	// status churn (retried before the first write) stay on.
	faults.failAfterCommit = func() bool { return false }
	kube := newSimKube(faults, simService(9999))
	cache := newSimCache(kube, faults)
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
	clock := newSimClock()
	loop := NewResponderLoop(IncidentRequest{IncidentID: "sim", Namespace: "app", SourceCommit: "tree-a", RepairPolicy: "recorded-actions", Findings: findings},
		registry, cache, []sdk.Detector{fastloopDetector{}}, PlanExecutor{
			Client: kube, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}, Observer: cache, Now: clock.Now,
		}, 5)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	loop.InitialDiagnose(context.Background())
	loop.Trust = map[string]ProgramTrust{"service-targetport-mismatch": {VerifiedSuccesses: 3, LastChange: "tree-a"}}
	return loop, kube
}

// TestSimTrustedFastPathNeverRollsBackAVerifiedFix: the cluster is repaired by
// a correct plan, so no read lag of the informer cache may make the fast path
// undo it (live incident 6: rolled back 14.5 ms after a correct write).
