package runtime

import (
	"context"
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func addedFieldsRef() sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "search"}
}

func addedFieldsSource() *unstructured.Unstructured {
	ref := addedFieldsRef()
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": ref.APIVersion, "kind": ref.Kind,
		"metadata": map[string]any{"namespace": ref.Namespace, "name": ref.Name, "labels": map[string]any{"app": "search"}},
		"spec": map[string]any{
			"replicas": int64(1),
			"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{
				"name": "search", "image": "search:1", "ports": []any{map[string]any{"containerPort": int64(8080)}},
			}}}},
		},
	}}
}

// addedFieldsLive is the accepted source as the API server stores it: server
// defaults filled in and accepted fields owned by the deployer's Apply.
func addedFieldsLive() *unstructured.Unstructured {
	live := addedFieldsSource()
	_ = unstructured.SetNestedField(live.Object, int64(600), "spec", "progressDeadlineSeconds")
	_ = unstructured.SetNestedField(live.Object, "ClusterFirst", "spec", "template", "spec", "dnsPolicy")
	containers, _, _ := unstructured.NestedSlice(live.Object, "spec", "template", "spec", "containers")
	container := containers[0].(map[string]any)
	container["terminationMessagePath"] = "/dev/termination-log"
	container["imagePullPolicy"] = "IfNotPresent"
	container["ports"].([]any)[0].(map[string]any)["protocol"] = "TCP"
	_ = unstructured.SetNestedSlice(live.Object, containers, "spec", "template", "spec", "containers")
	_ = unstructured.SetNestedField(live.Object, int64(1), "status", "replicas")
	acceptAddedFieldsLive(live)
	return live
}

// acceptAddedFieldsLive records the acceptance point the way adoption does:
// the record lists every path the accepted object carried.
func acceptAddedFieldsLive(live *unstructured.Unstructured) {
	record, fits, err := buildAcceptanceRecord(addedFieldsSource(), live, live, nil, false)
	if err != nil || !fits {
		panic(fmt.Sprintf("acceptance record: %v %v", fits, err))
	}
	annotations := live.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[acceptedPathsAnnotation] = record
	live.SetAnnotations(annotations)
}

func addAnnotation(live *unstructured.Unstructured, key, value string) {
	annotations := live.GetAnnotations()
	annotations[key] = value
	live.SetAnnotations(annotations)
}

func addedFieldsContainers(object *unstructured.Unstructured) []any {
	containers, _, _ := unstructured.NestedSlice(object.Object, "spec", "template", "spec", "containers")
	return containers
}

type viewResult struct {
	changed bool
	err     error
}

func setHostPort(live *unstructured.Unstructured) {
	containers := addedFieldsContainers(live)
	containers[0].(map[string]any)["ports"].([]any)[0].(map[string]any)["hostPort"] = int64(8080)
	_ = unstructured.SetNestedSlice(live.Object, containers, "spec", "template", "spec", "containers")
}

// Detection, repair and closure agree: every addition Changed reports is
// removed by accepted-source reconciliation, and the same comparison then
// reports the object restored. Map additions (labels, selectors) included.
func TestReconcileFromSourceClearsAddedFieldDrift(t *testing.T) {
	cases := map[string]func(*unstructured.Unstructured){
		"container hostPort": setHostPort,
		"annotation":         func(live *unstructured.Unstructured) { addAnnotation(live, "example.com/maintenance", "true") },
		"label": func(live *unstructured.Unstructured) {
			live.SetLabels(map[string]string{"app": "search", "tier": "canary"})
		},
		"node selector": func(live *unstructured.Unstructured) {
			_ = unstructured.SetNestedStringMap(live.Object, map[string]string{"disk": "ssd"}, "spec", "template", "spec", "nodeSelector")
		},
		"pod template label": func(live *unstructured.Unstructured) {
			_ = unstructured.SetNestedStringMap(live.Object, map[string]string{"canary": "true"}, "spec", "template", "metadata", "labels")
		},
	}
	for name, inject := range cases {
		t.Run(name, func(t *testing.T) {
			ref := addedFieldsRef()
			live := addedFieldsLive()
			inject(live)
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}
			diff := func() bool {
				t.Helper()
				snapshot := sdktest.Snapshot{NamespaceName: ref.Namespace, Objects: []unstructured.Unstructured{*client.objects[ref].DeepCopy()}}
				result, err := (LiveReadView{Carrier: inputs.New(snapshot), Source: planFakeSource{source: addedFieldsSource()}, SourceCommit: "accepted"}).DiffAgainstSource(ref)
				if err != nil {
					t.Fatal(err)
				}
				return result.Changed
			}
			if !diff() {
				t.Fatal("injected addition not reported as source drift")
			}
			executor := PlanExecutor{Client: client, Source: planFakeSource{source: addedFieldsSource()}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
			result, err := executor.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, false)
			if err != nil || !result.Applied {
				t.Fatalf("reconcile: %+v %v", result, err)
			}
			if diff() {
				t.Fatalf("reconciled object still reported as drift: %v %v", client.objects[ref].Object["metadata"], client.objects[ref].Object["spec"])
			}
			if _, ok := readAcceptanceRecord(client.objects[ref]); !ok {
				t.Fatal("reconciliation removed the acceptance record")
			}
		})
	}
}

// withAcceptance returns object's annotations plus the acceptance record that
// adoption would take against source over the object as it is now.
func withAcceptance(t testing.TB, object any, source *unstructured.Unstructured) map[string]string {
	t.Helper()
	annotations, err := acceptanceAnnotations(object, source)
	if err != nil {
		t.Fatal(err)
	}
	return annotations
}

func acceptanceAnnotations(object any, source *unstructured.Unstructured) (map[string]string, error) {
	live, ok := object.(*unstructured.Unstructured)
	if !ok {
		content, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(object)
		if err != nil {
			return nil, err
		}
		live = &unstructured.Unstructured{Object: content}
	}
	live = live.DeepCopy()
	if err := RecordAcceptance(live, source); err != nil {
		return nil, err
	}
	return live.GetAnnotations(), nil
}
