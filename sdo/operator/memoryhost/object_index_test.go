package memoryhost

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sync"
	"sync/atomic"
	"testing"
)

type countedObjectSnapshot struct {
	sdktest.Snapshot
	calls *atomic.Int32
}

func (s countedObjectSnapshot) Pods() []corev1.Pod { s.calls.Add(1); return s.Snapshot.Pods() }
func TestObjectIndexSharesProjectionAndReturnsPrivateCapturedObjects(t *testing.T) {
	var calls atomic.Int32
	view := countedObjectSnapshot{Snapshot: sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app", Labels: map[string]string{"origin": "typed"}}}}}, calls: &calls}
	captured := unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "api", "namespace": "app", "labels": map[string]any{"origin": "captured"}}}}
	index := NewObjectIndex(view, []unstructured.Unstructured{captured})
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api"}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			object, ok := index.Lookup(ref)
			if !ok || object.GetLabels()["origin"] != "captured" {
				t.Error("captured identity lost")
				return
			}
			object.SetLabels(map[string]string{"origin": "mutated"})
		})
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("projection calls=%d", calls.Load())
	}
	for _, missing := range []sdk.ObjectRef{{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "api"}, {APIVersion: "v2", Kind: "Pod", Namespace: "app", Name: "api"}, {APIVersion: "v1", Kind: "Pod", Namespace: "elsewhere", Name: "api"}} {
		if _, ok := index.Lookup(missing); ok {
			t.Fatal("index invented identity", missing)
		}
	}
}
