package runtime

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// A quiet extra controller and a deleting ConfigMap are latent faults: every
// availability/traffic detector can be clear. Their accepted-source absence
// must be observable even when they were present before the controller started.
func TestLatentSourceAbsentInitialObjectsAreUnexplained(t *testing.T) {
	source, revision := foreignSource(t)
	manifests, err := source.AcceptedManifests(context.Background(), revision, "app")
	if err != nil {
		t.Fatal(err)
	}
	deleted := metav1.NewTime(time.Unix(10, 0))
	deployment := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "extra-controller", Namespace: "app"}}
	config := corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cleanup-token", Namespace: "app", DeletionTimestamp: &deleted, Finalizers: []string{"example.test/cleanup"}}}
	snapshot := sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{deployment}, ConfigMapList: []corev1.ConfigMap{config}}
	index := NewSourceInventory("app", revision, manifests)
	objects := sdk.TypedObjects(snapshot)
	pointers := make([]*unstructured.Unstructured, 0, len(objects))
	for _, object := range objects {
		pointers = append(pointers, object.DeepCopy())
	}
	if findings := index.Findings(pointers, time.Unix(100, 0)); len(findings) != 2 {
		t.Fatalf("startup latent objects are invisible: %+v", findings)
	}
}
