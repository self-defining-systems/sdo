package sdktest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
)

func TestCaptureSnapshotBoundsNamespaceAndRedactsEnvironment(t *testing.T) {
	view := Snapshot{NamespaceName: "app", PodList: []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "broken", Namespace: "app"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Env: []corev1.EnvVar{{Name: "API_TOKEN", Value: "private"}, {Name: "MODE", Value: "live"}}}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "elsewhere"}},
	}, DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Env: []corev1.EnvVar{{Name: "PASSWORD", Value: "private"}}}}}}}}}}
	captured, err := CaptureSnapshot(view, "app")
	if err != nil {
		t.Fatal(err)
	}
	if len(captured.PodList) != 1 || captured.PodList[0].Name != "broken" || captured.PodList[0].Spec.Containers[0].Env[0].Value != "" || captured.PodList[0].Spec.Containers[0].Env[1].Value != "live" || captured.DeploymentList[0].Spec.Template.Spec.Containers[0].Env[0].Value != "" {
		t.Fatalf("snapshot did not preserve scoped, redacted state: %+v", captured)
	}
	if view.PodList[0].Spec.Containers[0].Env[0].Value != "private" {
		t.Fatal("capture mutated live view")
	}
	if _, err := CaptureSnapshot(view, "other"); err == nil || !strings.Contains(err.Error(), "namespace") {
		t.Fatalf("mismatched namespace accepted: %v", err)
	}
	var _ sdk.DetectionContext = captured
}

// Consumers of captures tell an in-place repair from a same-name recreation by
// metadata.uid, so identity survives capture while write bookkeeping does not.
func TestCaptureSnapshotRetainsObservationIdentityAndVersion(t *testing.T) {
	view := Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{
		Name: "api", Namespace: "app", ResourceVersion: "123", UID: "opaque", Generation: 7,
		ManagedFields: []metav1.ManagedFieldsEntry{{Manager: strings.Repeat("manager", 1000)}},
		Annotations:   map[string]string{"kubectl.kubernetes.io/last-applied-configuration": strings.Repeat("manifest", 1000), "kept": "value"},
	}}}}
	captured, err := CaptureSnapshot(view, "app")
	if err != nil {
		t.Fatal(err)
	}
	got := captured.PodList[0].ObjectMeta
	if got.ResourceVersion != "123" || got.UID != "opaque" || got.Generation != 0 || len(got.ManagedFields) != 0 || got.Annotations["kubectl.kubernetes.io/last-applied-configuration"] != "" || got.Annotations["kept"] != "value" {
		t.Fatalf("capture metadata: %+v", got)
	}
	if view.PodList[0].ResourceVersion != "123" {
		t.Fatal("capture changed live metadata")
	}
}

func TestCaptureSnapshotPreservesWorkloadRelations(t *testing.T) {
	view := Snapshot{NamespaceName: "app", StatefulSetList: []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Name: "database", Namespace: "app"}}, {ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "elsewhere"}}}, HPAList: []autoscalingv2.HorizontalPodAutoscaler{{ObjectMeta: metav1.ObjectMeta{Name: "scale", Namespace: "app"}, Spec: autoscalingv2.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "StatefulSet", Name: "database"}}}}, PDBList: []policyv1.PodDisruptionBudget{{ObjectMeta: metav1.ObjectMeta{Name: "budget", Namespace: "app"}, Spec: policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "database"}}}}}}
	captured, err := CaptureSnapshot(view, "app")
	if err != nil || len(captured.StatefulSetList) != 1 || len(captured.HPAList) != 1 || len(captured.PDBList) != 1 || captured.HPAList[0].Spec.ScaleTargetRef.Name != "database" || captured.PDBList[0].Spec.Selector.MatchLabels["app"] != "database" {
		t.Fatalf("relation objects missing from capture: %+v %v", captured, err)
	}
}

func TestCaptureSnapshotConfigMapsRetainKeysWithoutValues(t *testing.T) {
	const canary = "SHARED_CAPTURE_SYNTHETIC_CREDENTIAL_CANARY"
	immutable := true
	view := Snapshot{NamespaceName: "app", ConfigMapList: []corev1.ConfigMap{
		{ObjectMeta: metav1.ObjectMeta{Name: "kube-root-ca.crt", Namespace: "app", Labels: map[string]string{"app": "synthetic"}}, Data: map[string]string{"ca.crt": "-----BEGIN CERTIFICATE-----\n" + canary + "\n-----END CERTIFICATE-----", "empty": ""}, BinaryData: map[string][]byte{"private": []byte(canary)}, Immutable: &immutable},
		{ObjectMeta: metav1.ObjectMeta{Name: "empty", Namespace: "app"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "elsewhere"}, Data: map[string]string{"token": canary}},
	}}
	captured, err := CaptureSnapshot(view, "app")
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), canary) || strings.Contains(string(b), base64.StdEncoding.EncodeToString([]byte(canary))) {
		t.Fatal("ConfigMap credential retained in captured snapshot")
	}
	if len(captured.ConfigMapList) != 2 {
		t.Fatal("namespace changed")
	}
	var got, empty corev1.ConfigMap
	for _, object := range captured.ConfigMapList {
		if object.Name == "kube-root-ca.crt" {
			got = object
		}
		if object.Name == "empty" {
			empty = object
		}
	}
	if v, ok := got.Data["ca.crt"]; !ok || v != "" {
		t.Fatal("data key not retained empty")
	}
	if v, ok := got.BinaryData["private"]; !ok || len(v) != 0 {
		t.Fatal("binary key not retained empty")
	}
	if got.Name != "kube-root-ca.crt" || got.Labels["app"] != "synthetic" || got.Immutable == nil || !*got.Immutable {
		t.Fatal("ConfigMap identity changed")
	}
	if got.Annotations["sdo.dev/configmap-values-redacted"] != "true" {
		t.Fatal("redaction marker missing")
	}
	if !strings.Contains(view.ConfigMapList[0].Data["ca.crt"], canary) || string(view.ConfigMapList[0].BinaryData["private"]) != canary || view.ConfigMapList[0].Annotations["sdo.dev/configmap-values-redacted"] != "" {
		t.Fatal("capture mutated live values")
	}
	if empty.Data != nil || empty.BinaryData != nil {
		t.Fatal("empty ConfigMap acquired data")
	}
}

func TestCapturedConfigMapPresenceDiagnosisIsPreserved(t *testing.T) {
	for _, present := range []bool{false, true} {
		view := Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{deploymentWithConfigMapRef("app", "api", "settings", false)}}
		if present {
			view.ConfigMapList = []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "app"}, Data: map[string]string{"mode": "synthetic"}}}
		}
		captured, err := CaptureSnapshot(view, "app")
		if err != nil {
			t.Fatal(err)
		}
		before, err := detectMissingConfigMaps(context.Background(), view)
		if err != nil {
			t.Fatal(err)
		}
		after, err := detectMissingConfigMaps(context.Background(), captured)
		if err != nil {
			t.Fatal(err)
		}
		if len(before) != len(after) || (len(after) == 0) != present {
			t.Fatalf("key-only capture changed missing-ConfigMap diagnosis: present=%v", present)
		}
	}
}

func TestCaptureSnapshotRecordsObservationTimeForReplay(t *testing.T) {
	observed := time.Date(2026, 10, 23, 12, 0, 0, 123456789, time.UTC)
	captured, err := CaptureSnapshot(Snapshot{NamespaceName: "app", ObservedAtTime: &observed}, "app")
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	var replayed Snapshot
	if err := json.Unmarshal(data, &replayed); err != nil {
		t.Fatal(err)
	}
	if !replayed.ObservedAt().Equal(observed) || !sdk.ObservationTime(replayed).Equal(observed) {
		t.Fatalf("replayed observation time %v, want %v", replayed.ObservedAt(), observed)
	}
	unclocked, err := json.Marshal(Snapshot{NamespaceName: "app"})
	if err != nil || strings.Contains(string(unclocked), "observedAt") {
		t.Fatalf("a snapshot without a clock must not encode one: %s %v", unclocked, err)
	}
}
