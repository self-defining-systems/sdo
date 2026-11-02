//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// TRIAL1 hotel n2: the quota deletion was independently confirmed, then failed
// waiting for a ResourceQuota informer that does not exist. Exercise the real
// executor's direct-effect proof into the real cache observation boundary.
func TestConfirmedUncachedDeleteDoesNotWaitForMissingInformer(t *testing.T) {
	for _, identity := range []struct{ version, kind string }{{"v1", "ResourceQuota"}, {"v1", "LimitRange"}, {"example.test/v1", "SyntheticPolicy"}} {
		for _, remove := range []bool{true, false} {
			t.Run(identity.kind+"/"+map[bool]string{true: "delete", false: "patch"}[remove], func(t *testing.T) {
				ref := sdk.ObjectRef{APIVersion: identity.version, Kind: identity.kind, Namespace: "app", Name: "synthetic-policy"}
				object := planObject(ref, map[string]any{"hard": map[string]any{"memory": "1Gi"}})
				object.SetUID("synthetic-quota")
				object.SetResourceVersion("1")
				client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}}
				cache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset()}, nil)
				if err != nil {
					t.Fatal(err)
				}
				executor := PlanExecutor{Client: client, Observer: cache, Rollouts: generalNoRollouts{}, Verifier: generalHealthy{}}
				var action program.Action = program.DeleteObject{Ref: ref, Reason: "remove synthetic admission blocker"}
				if !remove {
					action = program.PatchObject{Ref: ref, Type: "merge", Patch: json.RawMessage(`{"metadata":{"labels":{"synthetic":"repaired"}}}`)}
				}
				result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{action}}, false)
				if err != nil || !result.Applied || !result.VerificationPassed || len(result.Patches) != 1 || !result.Patches[0].DirectReadConfirmed || remove && (client.deletes != 1 || len(result.ObjectEffects) != 1 || !result.ObjectEffects[0].Confirmed) {
					t.Fatalf("a directly confirmed effect waited on an unwatched kind: result=%+v deletes=%d err=%v", result, client.deletes, err)
				}
			})
		}
	}
}

// The complete production stage now consumes the live Match, applies the
// removal once, observes NoMatch and hands nothing to the agent. This joins
// finding -> candidate -> executor -> cache barrier -> stage consumer.
func TestStageDecisiveUncachedRemovalFinishesBeforeAgent(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ResourceQuota", Namespace: "app", Name: "synthetic-policy"}
	object := planObject(ref, map[string]any{"hard": map[string]any{"memory": "1Gi"}})
	object.SetUID("synthetic-policy")
	object.SetResourceVersion("1")
	client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}}
	source := lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{}}
	provider := uncachedPolicyProvider{client: client, source: source}
	detector := uncachedPolicyDetector{ref: ref}
	snapshot, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	findings, err := detector.Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	p := uncachedPolicyProgram{}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	cache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := goldenRequest(t)
	request.Namespace, request.Findings, request.RepairPolicy = "app", findings, "recorded-actions"
	request.ResponseDeadline = time.Now().Add(time.Minute)
	executor := PlanExecutor{Client: client, Source: source, Observer: cache, Rollouts: generalNoRollouts{}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{detector}, Provider: provider}}
	loop := NewResponderLoop(request, registry, provider, []sdk.Detector{detector}, executor, 5)
	loop.ReadView = program.ReadFrom
	state := &IncidentState{Request: request, Findings: findings}
	agentCalls := 0
	if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}, clearingResponseStage{name: "agent", calls: &agentCalls}}); err != nil {
		t.Fatal(err)
	}
	if agentCalls != 0 || client.deletes != 1 || len(state.Findings) != 0 || len(state.PriorStages) != 1 || len(state.PriorStages[0].Mitigations) != 1 || state.PriorStages[0].Mitigations[0].Result != "applied_cleared" {
		t.Fatalf("decisive match reached the agent: calls=%d deletes=%d state=%+v", agentCalls, client.deletes, state)
	}
}

func TestUncachedWriteStillNeedsFreshDirectProofAndWatchedKindsWait(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "synthetic-policy"}
	cache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset()}, []sdk.Detector{scheduledDetector{spec: sdk.DetectorSpec{ID: "config", Interval: time.Second, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "ConfigMap"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if cache.DirectPlanReadSuffices(ref) {
		t.Fatal("direct proof bypassed a watched verifier input")
	}
	ref.Kind = "ResourceQuota"
	patch := PlanPatch{Ref: ref, Structured: true, DirectReadConfirmed: true, After: planObject(ref, nil)}
	patch.After.SetResourceVersion("2")
	patch.After.SetUID("synthetic-policy")
	// A persisted receipt cannot restore process-local read authority.
	raw, err := json.Marshal(patch)
	if err != nil {
		t.Fatal(err)
	}
	var restored PlanPatch
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (PlanExecutor{Observer: cache}).awaitObserved(ctx, []PlanPatch{restored}); err == nil {
		t.Fatal("unconfirmed uncached write passed the barrier")
	}
	if err := (PlanExecutor{Observer: cache}).awaitObserved(ctx, []PlanPatch{patch}); err != nil {
		t.Fatalf("fresh direct proof did not discharge an uncached barrier: %v", err)
	}
}
