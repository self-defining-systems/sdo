package sdktest_test

import (
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk/sdktest"
)

// A namespace whose objects exceed the snapshot budget is captured with the
// least relevant objects trimmed, and the trimmed counts are recorded; the
// objects detectors read most (pods, services) stay.
func TestCaptureTrimsLeastRelevantObjectsInsteadOfFailing(t *testing.T) {
	source := sdktest.Snapshot{NamespaceName: "app", ObjectSnapshotVersion: 1}
	source.PodList = []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app"}}}
	source.ServiceList = []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app"}}}
	blob := strings.Repeat("x", 512<<10)
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q"} {
		source.ConfigMapList = append(source.ConfigMapList, corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "cfg-" + name, Namespace: "app"}, Data: map[string]string{"k": blob}})
	}
	// ConfigMap data keeps key names only, so inflate with labels, which survive projection.
	for i := range source.ConfigMapList {
		source.ConfigMapList[i].Data = nil
		source.ConfigMapList[i].Labels = map[string]string{"blob": blob}
	}
	snapshot, err := sdktest.CaptureSnapshot(source, "app")
	if err != nil {
		t.Fatalf("capture must degrade, not fail: %v", err)
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > sdktest.MaxCapturedSnapshotBytes {
		t.Fatalf("snapshot is %d bytes, over the %d budget", len(raw), sdktest.MaxCapturedSnapshotBytes)
	}
	if snapshot.TrimmedObjects["ConfigMap"] == 0 {
		t.Fatalf("trimmed = %v, want ConfigMaps counted", snapshot.TrimmedObjects)
	}
	if len(snapshot.Pods()) != 1 || len(snapshot.Services()) != 1 {
		t.Fatalf("pods = %d, services = %d: relevant objects were trimmed", len(snapshot.Pods()), len(snapshot.Services()))
	}
	if kept := len(snapshot.ConfigMaps()); kept+snapshot.TrimmedObjects["ConfigMap"] != len(source.ConfigMapList) {
		t.Fatalf("kept %d + trimmed %d != %d", kept, snapshot.TrimmedObjects["ConfigMap"], len(source.ConfigMapList))
	}
}

// Objects that are never trimmed still fail loudly when they alone exceed the budget.
func TestCaptureFailsWhenProtectedObjectsExceedBudget(t *testing.T) {
	source := sdktest.Snapshot{NamespaceName: "app", ObjectSnapshotVersion: 1}
	blob := strings.Repeat("x", 1<<20)
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"} {
		source.PodList = append(source.PodList, corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", Labels: map[string]string{"blob": blob}}})
	}
	if _, err := sdktest.CaptureSnapshot(source, "app"); err == nil {
		t.Fatal("a snapshot of only pods over the budget must fail")
	}
}
