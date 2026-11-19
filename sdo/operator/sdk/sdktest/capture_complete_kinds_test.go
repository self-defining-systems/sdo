package sdktest_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

var (
	configMaps = schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}
	services   = schema.GroupVersionKind{Version: "v1", Kind: "Service"}
)

// A capture keeps which kinds its source listed completely, so a replay can say
// an object is absent exactly where the live read could have.
func TestCaptureKeepsTheKindsItsSourceListedCompletely(t *testing.T) {
	source := sdktest.Snapshot{NamespaceName: "app", ObjectSnapshotVersion: 1, CompleteKindList: []schema.GroupVersionKind{configMaps}}
	source.ConfigMapList = []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "app"}}}
	captured, err := sdktest.CaptureSnapshot(source, "app")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	var loaded sdktest.Snapshot
	if err := json.Unmarshal(raw, &loaded); err != nil {
		t.Fatal(err)
	}
	if _, err := loaded.Object(sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "gone"}); !errors.Is(err, sdk.ErrObjectAbsent) {
		t.Fatalf("a complete ConfigMap list lost in capture: %v", err)
	}
	if _, err := loaded.Object(sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "gone"}); !errors.Is(err, sdk.ErrObjectNotCaptured) {
		t.Fatalf("an unlisted kind must stay not captured: %v", err)
	}
}

// A legacy capture, with no complete kinds, asserts absence for no kind.
func TestCaptureOfASourceWithoutCompleteKindsAssertsNoAbsence(t *testing.T) {
	captured, err := sdktest.CaptureSnapshot(sdktest.Snapshot{NamespaceName: "app", ObjectSnapshotVersion: 1}, "app")
	if err != nil {
		t.Fatal(err)
	}
	if len(captured.CompleteKindList) != 0 {
		t.Fatalf("complete kinds = %v", captured.CompleteKindList)
	}
}

// A kind whose objects the capture trimmed to fit its budget is not complete.
func TestCaptureDoesNotVouchForAKindItTrimmed(t *testing.T) {
	source := sdktest.Snapshot{NamespaceName: "app", ObjectSnapshotVersion: 1, CompleteKindList: []schema.GroupVersionKind{configMaps, services}}
	source.ServiceList = []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app"}}}
	blob := strings.Repeat("x", 512<<10)
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q"} {
		source.ConfigMapList = append(source.ConfigMapList, corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cfg-" + name, Namespace: "app", Labels: map[string]string{"blob": blob}}})
	}
	captured, err := sdktest.CaptureSnapshot(source, "app")
	if err != nil {
		t.Fatal(err)
	}
	if captured.TrimmedObjects["ConfigMap"] == 0 {
		t.Fatal("the fixture did not trim")
	}
	for _, kind := range captured.CompleteKindList {
		if kind.Kind == "ConfigMap" {
			t.Fatalf("a trimmed kind is still complete: %v", captured.CompleteKindList)
		}
	}
	if len(captured.CompleteKindList) != 1 || captured.CompleteKindList[0].Kind != "Service" {
		t.Fatalf("complete kinds = %v, want only Service", captured.CompleteKindList)
	}
}
