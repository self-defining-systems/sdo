package runtime

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// GL1b used a benchmark application name different from the deployed identity.
// No administrator app-name mapping or content attestation is needed to repair it.
func TestGS1DeployedIdentityAndNamespaceScope(t *testing.T) {
	for _, namespace := range []string{"gallery-live", "sdo-control", "another-app"} {
		t.Run(namespace, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: namespace, Name: "deployed-config"}
			before := planObject(ref, nil)
			before.SetUID("config-uid")
			before.SetResourceVersion("1")
			before.Object["data"] = map[string]any{"mode": "broken"}
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}
			executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			incident := IncidentRequest{IncidentID: "gl1b", Application: "sregym-gallery", Namespace: "gallery-live", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}
			plan := program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "data[mode]", Value: testPlanValue("working")}}}
			result, err := executor.Apply(context.Background(), incident, plan, false)
			if namespace != incident.Namespace {
				if err == nil || len(client.patches) != 0 {
					t.Fatalf("outside scope was mutated: %+v %v", result, err)
				}
				return
			}
			if err != nil || !result.VerificationPassed {
				t.Fatalf("in-scope deployed identity refused: %+v %v", result, err)
			}
			if err := executor.Rollback(context.Background(), result); err != nil {
				t.Fatal(err)
			}
			if !planValuesEqual(before.Object, client.objects[ref].Object) {
				t.Fatal("pre-image not restored")
			}
		})
	}
}

func TestGS1LifecycleKindsAndAcceptedSourceDelete(t *testing.T) {
	for _, create := range []bool{false, true} {
		ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "accepted-workload"}
		if create {
			ref = sdk.ObjectRef{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "accepted-policy"}
		}
		object := planObject(ref, map[string]any{})
		object.SetUID("existing-uid")
		object.SetResourceVersion("1")
		client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}}
		var action program.Action = program.DeleteObject{Ref: ref, Reason: "replace a source-present failing workload"}
		if create {
			delete(client.objects, ref)
			action = program.CreateFromSource{Ref: ref, Reason: "restore a missing accepted policy"}
		}
		source := lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}
		executor := PlanExecutor{Client: client, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
		incident := IncidentRequest{Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}
		result, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{action}}, false)
		if err != nil || !result.Applied || !result.VerificationPassed {
			t.Fatalf("create=%t: %+v %v", create, result, err)
		}
		if create {
			if err := executor.Rollback(context.Background(), result); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := executor.Rollback(context.Background(), result); err == nil || client.creates != 0 {
				t.Fatal("delete was not disclosed as irreversible")
			}
		}
	}
}

