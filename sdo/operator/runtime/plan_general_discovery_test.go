package runtime

import (
	"context"
	"encoding/json"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	discoveryfake "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
	"time"
)

func generalDiscoveryClient(ref sdk.ObjectRef, kindResource string, complete bool) GenericPlanClient {
	dynamic := dynamicfake.NewSimpleDynamicClient(kruntime.NewScheme())
	dynamic.PrependReactor("create", "*", func(action clienttesting.Action) (bool, kruntime.Object, error) {
		o := action.(clienttesting.CreateAction).GetObject().(*unstructured.Unstructured)
		o.SetUID("server-uid")
		o.SetResourceVersion("1")
		if complete {
			o.Object["status"] = map[string]any{"succeeded": int64(1)}
		}
		return false, nil, nil
	})
	discovery := &discoveryfake.FakeDiscovery{Fake: &clienttesting.Fake{Resources: []*metav1.APIResourceList{{GroupVersion: ref.APIVersion, APIResources: []metav1.APIResource{{Kind: ref.Kind, Name: kindResource, Namespaced: ref.Namespace != "", Verbs: []string{"get", "list", "patch", "create", "delete"}}}}}}}
	return GenericPlanClient{Objects: &core.GenericCapture{Client: dynamic, Discovery: discovery, Namespace: "app"}}
}
func TestGeneralDiscoverableArbitraryObject(t *testing.T) {
	for _, namespace := range []string{"app", "other", ""} {
		t.Run("namespace="+namespace, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "example.test/v9", Kind: "NewKind", Namespace: namespace, Name: "policy"}
			metadata := map[string]any{"name": ref.Name}
			if namespace != "" {
				metadata["namespace"] = namespace
			}
			desired := map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": metadata, "spec": map[string]any{"state": "before"}}
			c := generalDiscoveryClient(ref, "newkinds", false)
			e := PlanExecutor{Client: c, Rollouts: generalNoRollouts{}, Verifier: generalHealthy{}}
			incident := IncidentRequest{Namespace: "app"}
			p := program.Plan{Actions: []program.Action{program.ApplyObject{Ref: ref, Mode: "create", Object: desired}, program.PatchObject{Ref: ref, Type: "merge", Patch: json.RawMessage(`{"spec":{"state":"after"}}`)}}}
			if r, err := e.Apply(context.Background(), incident, p, false); err != nil || !r.VerificationPassed {
				t.Fatalf("arbitrary object: %+v %v", r, err)
			}
			if r, err := e.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.DeleteObject{Ref: ref, Reason: "fixture"}}}, false); err != nil || !r.VerificationPassed {
				t.Fatalf("arbitrary delete: %+v %v", r, err)
			}
		})
	}
}
func TestGeneralJobProductionRecordsObservedStatus(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "batch/v1", Kind: "Job", Namespace: "app", Name: "once"}
	c := generalDiscoveryClient(ref, "jobs", true)
	e := PlanExecutor{Client: c, Rollouts: generalNoRollouts{}, Verifier: generalHealthy{}}
	desired := map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace}, "spec": map[string]any{"template": map[string]any{"spec": map[string]any{"restartPolicy": "Never", "containers": []any{map[string]any{"name": "script", "image": "fixture", "command": []any{"true"}}}}}}}
	p := program.Plan{Actions: []program.Action{program.RunJob{Object: program.ApplyObject{Ref: ref, Object: desired}, Completion: program.WaitFor{Ref: ref, Path: "/status/succeeded", Equals: int64(1), Timeout: time.Second}}}}
	r, err := e.Apply(context.Background(), IncidentRequest{}, p, false)
	if err != nil || !r.VerificationPassed || len(r.ActionReports[0].JobStatus) == 0 || !r.Patches[0].Irreversible {
		t.Fatalf("job production: %+v %v", r, err)
	}
}
