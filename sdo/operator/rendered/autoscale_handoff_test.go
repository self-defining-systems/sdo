package rendered_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/rendered"
)

func liveWith(replicas any, entries ...metav1.ManagedFieldsEntry) *unstructured.Unstructured {
	live := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "app"}, "spec": map[string]any{"replicas": replicas}}}
	live.SetManagedFields(entries)
	return live
}

func entry(manager string, op metav1.ManagedFieldsOperationType, fields string) metav1.ManagedFieldsEntry {
	return metav1.ManagedFieldsEntry{Manager: manager, Operation: op, FieldsV1: &metav1.FieldsV1{Raw: []byte(fields)}}
}

func TestReplicasHandoffDocumentCarriesOnlyTheLiveCount(t *testing.T) {
	for _, replicas := range []any{int64(3), float64(3), int32(3)} {
		document, ok := rendered.ReplicasHandoff(liveWith(replicas))
		if !ok {
			t.Fatalf("%T: no document", replicas)
		}
		got, _, _ := unstructured.NestedInt64(document.Object, "spec", "replicas")
		if got != 3 || len(document.Object) != 4 || document.GetName() != "web" || document.GetNamespace() != "app" {
			t.Fatalf("%T: document = %v", replicas, document.Object)
		}
	}
	if _, ok := rendered.ReplicasHandoff(liveWith(nil)); ok {
		t.Fatal("a live object without a count needs no hand-off")
	}
}

func TestSolelyOwnsReplicas(t *testing.T) {
	owns := `{"f:spec":{"f:replicas":{},"f:selector":{}}}`
	other := `{"f:spec":{"f:selector":{}}}`
	for name, tc := range map[string]struct {
		entries []metav1.ManagedFieldsEntry
		want    bool
	}{
		"deployer alone":              {[]metav1.ManagedFieldsEntry{entry("sdo-deployer", metav1.ManagedFieldsOperationApply, owns)}, true},
		"no owner of the count":       {[]metav1.ManagedFieldsEntry{entry("sdo-deployer", metav1.ManagedFieldsOperationApply, other)}, false},
		"shared with a neutral owner": {[]metav1.ManagedFieldsEntry{entry("sdo-deployer", metav1.ManagedFieldsOperationApply, owns), entry(rendered.ReplicasHandoffManager, metav1.ManagedFieldsOperationApply, owns)}, false},
		"autoscaler owns it":          {[]metav1.ManagedFieldsEntry{entry("kube-controller-manager", metav1.ManagedFieldsOperationUpdate, owns)}, false},
	} {
		if got := rendered.SolelyOwnsReplicas(liveWith(int64(3), tc.entries...), "sdo-deployer"); got != tc.want {
			t.Errorf("%s: %v, want %v", name, got, tc.want)
		}
	}
}
