package runtime

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

var (
	addedFieldsCreated  = metav1.NewTime(time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC))
	addedFieldsAccepted = metav1.NewTime(time.Date(2026, 10, 26, 11, 0, 0, 0, time.UTC))
	addedFieldsDrifted  = metav1.NewTime(time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC))
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
	live.SetManagedFields([]metav1.ManagedFieldsEntry{
		addedFieldsEntry("sdo-deployer", metav1.ManagedFieldsOperationApply, "", addedFieldsAccepted,
			`{"f:metadata":{"f:labels":{"f:app":{}}},"f:spec":{"f:replicas":{},"f:template":{"f:spec":{"f:containers":{"k:{\"name\":\"search\"}":{".":{},"f:image":{},"f:name":{},"f:ports":{"k:{\"containerPort\":8080,\"protocol\":\"TCP\"}":{".":{},"f:containerPort":{}}}}}}}}}`),
		addedFieldsEntry("kube-controller-manager", metav1.ManagedFieldsOperationUpdate, "status", addedFieldsDrifted, `{"f:status":{"f:replicas":{}}}`),
	})
	return live
}

func addedFieldsEntry(manager string, operation metav1.ManagedFieldsOperationType, subresource string, at metav1.Time, fields string) metav1.ManagedFieldsEntry {
	return metav1.ManagedFieldsEntry{Manager: manager, Operation: operation, Subresource: subresource, APIVersion: "apps/v1", Time: &at, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(fields)}}
}

func addManagedEntry(object *unstructured.Unstructured, entry metav1.ManagedFieldsEntry) {
	object.SetManagedFields(append(object.GetManagedFields(), entry))
}

func addedFieldsContainers(object *unstructured.Unstructured) []any {
	containers, _, _ := unstructured.NestedSlice(object.Object, "spec", "template", "spec", "containers")
	return containers
}

// diffBothNormalizations compares without a client and with a server-side
// apply dry-run that, like the API server, keeps every other manager's fields.
func diffBothNormalizations(t *testing.T, live *unstructured.Unstructured) map[string]bool {
	t.Helper()
	ref := addedFieldsRef()
	// Read the object back as the API server serves it.
	var typed appsv1.Deployment
	if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(live.Object, &typed); err != nil {
		t.Fatal(err)
	}
	content, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(&typed)
	if err != nil {
		t.Fatal(err)
	}
	live = &unstructured.Unstructured{Object: content}
	live.SetAPIVersion(ref.APIVersion)
	live.SetKind(ref.Kind)
	snapshot := sdktest.Snapshot{NamespaceName: ref.Namespace, Objects: []unstructured.Unstructured{*live}}
	results := map[string]bool{}
	for _, name := range []string{"snapshot", "apply-dry-run"} {
		view := LiveReadView{DetectionContext: snapshot, Source: planFakeSource{source: addedFieldsSource()}, SourceCommit: "accepted"}
		if name == "apply-dry-run" {
			client := fake.NewSimpleClientset()
			client.PrependReactor("patch", "deployments", func(ktesting.Action) (bool, kruntime.Object, error) {
				return true, typed.DeepCopy(), nil
			})
			view.Client = client
		}
		diff, err := view.DiffAgainstSource(ref)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		results[name] = diff.Changed
	}
	return results
}

func TestSourceDiffChangedForFieldsAddedAfterAcceptance(t *testing.T) {
	cases := map[string]func(*unstructured.Unstructured){
		"container hostPort": func(live *unstructured.Unstructured) {
			containers := addedFieldsContainers(live)
			containers[0].(map[string]any)["ports"].([]any)[0].(map[string]any)["hostPort"] = int64(8080)
			_ = unstructured.SetNestedSlice(live.Object, containers, "spec", "template", "spec", "containers")
			addManagedEntry(live, addedFieldsEntry("kubectl-patch", metav1.ManagedFieldsOperationUpdate, "", addedFieldsDrifted,
				`{"f:spec":{"f:template":{"f:spec":{"f:containers":{"k:{\"name\":\"search\"}":{"f:ports":{"k:{\"containerPort\":8080,\"protocol\":\"TCP\"}":{"f:hostPort":{}}}}}}}}}`))
		},
		"sidecar container": func(live *unstructured.Unstructured) {
			containers := append(addedFieldsContainers(live), map[string]any{"name": "proxy", "image": "proxy:1", "terminationMessagePath": "/dev/termination-log"})
			_ = unstructured.SetNestedSlice(live.Object, containers, "spec", "template", "spec", "containers")
			addManagedEntry(live, addedFieldsEntry("kubectl-patch", metav1.ManagedFieldsOperationUpdate, "", addedFieldsDrifted,
				`{"f:spec":{"f:template":{"f:spec":{"f:containers":{"k:{\"name\":\"proxy\"}":{".":{},"f:image":{},"f:name":{},"f:terminationMessagePath":{}}}}}}}`))
		},
		"annotation": func(live *unstructured.Unstructured) {
			live.SetAnnotations(map[string]string{"example.com/maintenance": "true"})
			addManagedEntry(live, addedFieldsEntry("kubectl-annotate", metav1.ManagedFieldsOperationUpdate, "", addedFieldsDrifted,
				`{"f:metadata":{"f:annotations":{".":{},"f:example.com/maintenance":{}}}}`))
		},
		"label": func(live *unstructured.Unstructured) {
			live.SetLabels(map[string]string{"app": "search", "tier": "canary"})
			addManagedEntry(live, addedFieldsEntry("kubectl-label", metav1.ManagedFieldsOperationUpdate, "", addedFieldsDrifted,
				`{"f:metadata":{"f:labels":{"f:tier":{}}}}`))
		},
		"pod template field": func(live *unstructured.Unstructured) {
			_ = unstructured.SetNestedStringMap(live.Object, map[string]string{"disk": "ssd"}, "spec", "template", "spec", "nodeSelector")
			addManagedEntry(live, addedFieldsEntry("chaos", metav1.ManagedFieldsOperationApply, "", addedFieldsDrifted,
				`{"f:spec":{"f:template":{"f:spec":{"f:nodeSelector":{"f:disk":{}}}}}}`))
		},
	}
	for name, inject := range cases {
		t.Run(name, func(t *testing.T) {
			live := addedFieldsLive()
			inject(live)
			for normalization, changed := range diffBothNormalizations(t, live) {
				if !changed {
					t.Errorf("%s: added field drift reported unchanged", normalization)
				}
			}
		})
	}
}

func TestSourceDiffUnchangedForServerDefaultsAndBookkeeping(t *testing.T) {
	cases := map[string]func(*unstructured.Unstructured){
		"applied defaults": func(*unstructured.Unstructured) {},
		// Adopted objects: the original creator's Update owns server defaults,
		// written before SDO accepted the object.
		"adopted creator defaults": func(live *unstructured.Unstructured) {
			addManagedEntry(live, addedFieldsEntry("kubectl-create", metav1.ManagedFieldsOperationUpdate, "", addedFieldsCreated,
				`{"f:spec":{"f:progressDeadlineSeconds":{},"f:template":{"f:spec":{"f:dnsPolicy":{},"f:containers":{"k:{\"name\":\"search\"}":{"f:imagePullPolicy":{},"f:terminationMessagePath":{},"f:ports":{"k:{\"containerPort\":8080,\"protocol\":\"TCP\"}":{"f:protocol":{}}}}}}}}}`))
		},
		"kubernetes bookkeeping annotations": func(live *unstructured.Unstructured) {
			live.SetAnnotations(map[string]string{"deployment.kubernetes.io/revision": "3"})
			_ = unstructured.SetNestedStringMap(live.Object, map[string]string{"kubectl.kubernetes.io/restartedAt": "2026-10-26T12:00:00Z"}, "spec", "template", "metadata", "annotations")
			addManagedEntry(live, addedFieldsEntry("kube-controller-manager", metav1.ManagedFieldsOperationUpdate, "", addedFieldsDrifted,
				`{"f:metadata":{"f:annotations":{".":{},"f:deployment.kubernetes.io/revision":{}}}}`))
			addManagedEntry(live, addedFieldsEntry("kubectl-rollout", metav1.ManagedFieldsOperationUpdate, "", addedFieldsDrifted,
				`{"f:spec":{"f:template":{"f:metadata":{"f:annotations":{".":{},"f:kubectl.kubernetes.io/restartedAt":{}}}}}}`))
		},
		"foreign write to an accepted field it restored": func(live *unstructured.Unstructured) {
			addManagedEntry(live, addedFieldsEntry("kubectl-patch", metav1.ManagedFieldsOperationUpdate, "", addedFieldsDrifted,
				`{"f:spec":{"f:replicas":{}}}`))
		},
	}
	for name, prepare := range cases {
		t.Run(name, func(t *testing.T) {
			live := addedFieldsLive()
			prepare(live)
			for normalization, changed := range diffBothNormalizations(t, live) {
				if changed {
					t.Errorf("%s: unchanged object reported as drift", normalization)
				}
			}
		})
	}
}
