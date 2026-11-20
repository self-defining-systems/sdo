package runtime

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// A Pod that is 15 s old when the source restore lands is neither failing with
// a stuck reason nor past the not-Ready bound, so the apply-time check leaves
// it; a StatefulSet never replaces a Pod that is not Ready. The recovery wait
// re-checks, and the Pod is replaced once it is stuck.
func TestSourceRestoreRechecksAPodThatWasTooYoungAtApplyTime(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	clock := now
	live := syntheticSet("registry.invalid/store:bad", nil)
	accepted := syntheticSet("registry.invalid/store:good", nil)
	manifest, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(accepted)
	if err != nil {
		t.Fatal(err)
	}
	pod := syntheticStorePod(live, stuckPod{name: "store-0", revision: "store-old", notReadyFor: 15 * time.Second}, now)
	client := fake.NewSimpleClientset(live, pod)
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "StatefulSet", Namespace: "app", Name: "store"}
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Source: templateSource{&unstructured.Unstructured{Object: manifest}},
		Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{}, Now: func() time.Time { return clock }}
	incident := IncidentRequest{IncidentID: "synthetic", Namespace: "app", RepairPolicy: "recorded-actions", SourceCommit: "0123456789abcdef0123456789abcdef01234567",
		Findings: []sdk.Finding{{RuleID: "synthetic", PrimaryResource: ref}}}
	result, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, false)
	if err != nil || !result.Applied {
		t.Fatalf("Apply = %+v, %v", result, err)
	}
	pods := func() int {
		list, _ := client.CoreV1().Pods("app").List(context.Background(), metav1.ListOptions{})
		return len(list.Items)
	}
	if pods() != 1 || len(result.ObjectEffects) != 0 {
		t.Fatalf("the apply-time check deleted a 15 s old Pod: %d Pods, effects %+v", pods(), result.ObjectEffects)
	}
	loop := &ResponderLoop{Request: incident, Executor: executor, applications: map[string]PlanApplicationResult{"app-1": result}}
	loop.applicationsLog = []RecordedPlanApplication{{ID: "app-1", Result: result}}

	clock = now.Add(10 * time.Second)
	loop.replaceStuckRevisionPods(context.Background())
	if pods() != 1 {
		t.Fatal("a Pod not yet past the bound was deleted")
	}
	clock = now.Add(20 * time.Second)
	loop.replaceStuckRevisionPods(context.Background())
	if pods() != 0 {
		t.Fatal("the recheck did not replace the Pod stuck on the old revision")
	}
	effects := loop.applicationsLog[0].Result.ObjectEffects
	if len(effects) != 1 || effects[0].Ref.Name != "store-0" || !effects[0].Confirmed {
		t.Fatalf("the replacement is not recorded on the application: %+v", effects)
	}
	if got := loop.applications["app-1"].ObjectEffects; len(got) != 1 {
		t.Fatalf("live application lacks the effect: %+v", got)
	}
	loop.replaceStuckRevisionPods(context.Background())
	if n := len(loop.applicationsLog[0].Result.ObjectEffects); n != 1 {
		t.Fatalf("a repeated recheck recorded %d effects", n)
	}
}
