package replay

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func configMap(name string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: name}
}

// With no recording of the comparison, replay answers from the case's accepted
// inventory through the live view's presence rule: a corpus case that captured
// the complete ConfigMap list says a deleted declared object is a diff and an
// undeclared one absent on both sides; a legacy case without complete kinds says
// nothing, as before.
func TestReplayOfflineFallbackAnswersAbsenceLikeLive(t *testing.T) {
	declared := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": "cm", "namespace": "app"},
		"data":     map[string]any{"init.sh": "echo hi"},
	}}
	src := &source{accepted: map[sdk.ObjectRef]*unstructured.Unstructured{configMap("cm"): declared}, revisions: map[string]map[sdk.ObjectRef]*unstructured.Unstructured{}}
	list := []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "app"}}}

	complete := sdktest.Snapshot{NamespaceName: "app", ConfigMapList: list, CompleteKindList: []schema.GroupVersionKind{{Version: "v1", Kind: "ConfigMap"}}}
	view := newReadView(context.Background(), complete, nil, src)
	if diff, err := view.DiffAgainstSource(configMap("cm")); err != nil || !diff.MissingButInSource() {
		t.Fatalf("declared deleted = %+v, %v", diff, err)
	}
	if diff, err := view.DiffAgainstSource(configMap("nowhere")); err != nil || !diff.SourceAbsent || !diff.LiveAbsent || diff.Changed {
		t.Fatalf("undeclared absent = %+v, %v", diff, err)
	}

	legacy := sdktest.Snapshot{NamespaceName: "app", ConfigMapList: list}
	old := newReadView(context.Background(), legacy, nil, src)
	if _, err := old.DiffAgainstSource(configMap("cm")); err == nil {
		t.Fatal("a case without complete kinds asserted an absence")
	}
}
