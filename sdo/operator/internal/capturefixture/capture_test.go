package capturefixture

import (
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk/sdktest"
)

func liveService(name string) corev1.Service {
	return corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo", UID: types.UID("uid-" + name),
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubectl", Operation: metav1.ManagedFieldsOperationApply}},
			Annotations:   map[string]string{"kubectl.kubernetes.io/last-applied-configuration": "{}"}},
	}
}

func TestBoundaryUsesCaptureProjection(t *testing.T) {
	raw := Boundary(t, sdktest.Snapshot{NamespaceName: "demo", ServiceList: []corev1.Service{liveService("front")}}, "demo")
	text := string(raw)
	if strings.Contains(text, "managedFields") || strings.Contains(text, "last-applied-configuration") {
		t.Fatalf("boundary skipped the capture projection: %s", text)
	}
	if !strings.Contains(text, `"uid":"uid-front"`) || !strings.Contains(text, `"object_snapshot_version":1`) {
		t.Fatalf("boundary lost captured identity or version: %s", text)
	}
}

func TestRecaptureKeepsEditedTypedListsAuthoritative(t *testing.T) {
	raw := Boundary(t, sdktest.Snapshot{NamespaceName: "demo", ServiceList: []corev1.Service{liveService("front"), liveService("peer")}}, "demo")
	edited := Recapture(t, raw, func(s *sdktest.Snapshot) { s.ServiceList = s.ServiceList[:1] })
	var snapshot sdktest.Snapshot
	if err := json.Unmarshal(edited, &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.ServiceList) != 1 || len(snapshot.Objects) != 1 || snapshot.Objects[0].GetName() != "front" {
		t.Fatalf("removed Service survived recapture: services=%v objects=%v", snapshot.ServiceList, snapshot.Objects)
	}
}
