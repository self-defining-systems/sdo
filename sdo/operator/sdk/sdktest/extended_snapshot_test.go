package sdktest

import (
	"encoding/json"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
)

func TestSecretMetadataNeverContainsValues(t *testing.T) {
	secret := corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "demo", Annotations: map[string]string{"applied": "sensitive-value"}, Labels: map[string]string{"app": "a"}}, Data: map[string][]byte{"z": []byte("sensitive-value"), "a": []byte("second-value")}, StringData: map[string]string{"b": "third-value"}}
	metadata := sdk.SecretMetadataFrom(secret)
	if !reflect.DeepEqual(metadata.DataKeys, []string{"a", "b", "z"}) {
		t.Fatalf("keys: %v", metadata.DataKeys)
	}
	s := Snapshot{NamespaceName: "demo", SecretList: []sdk.SecretMetadata{metadata}}
	copies := s.Secrets()
	copies[0].Labels["app"] = "changed"
	copies[0].DataKeys[0] = "changed"
	if s.Secrets()[0].Labels["app"] != "a" || s.Secrets()[0].DataKeys[0] != "a" {
		t.Fatal("mutable accessor")
	}
	captured, err := CaptureSnapshot(s, "demo")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{metadata, s, captured} {
		b, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var document any
		if err = json.Unmarshal(b, &document); err != nil {
			t.Fatal(err)
		}
		check := string(b)
		for _, bad := range []string{"sensitive-value", "second-value", "third-value", "c2Vuc2l0aXZlLXZhbHVl", "annotations", "stringData"} {
			if contains(check, bad) {
				t.Fatalf("leaked %s: %s", bad, b)
			}
		}
	}
}
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
func TestExtendedAccessors(t *testing.T) {
	s := Snapshot{}
	var view sdk.DetectionContext = s
	if len(view.StatefulSets()) != 0 {
		t.Fatal("StatefulSets")
	}
	if len(view.DaemonSets()) != 0 {
		t.Fatal("DaemonSets")
	}
	if len(view.Jobs()) != 0 {
		t.Fatal("Jobs")
	}
	if len(view.CronJobs()) != 0 {
		t.Fatal("CronJobs")
	}
	if len(view.PersistentVolumeClaims()) != 0 {
		t.Fatal("PersistentVolumeClaims")
	}
	if len(view.ServiceAccounts()) != 0 {
		t.Fatal("ServiceAccounts")
	}
	if len(view.HorizontalPodAutoscalers()) != 0 {
		t.Fatal("HorizontalPodAutoscalers")
	}
	if len(view.PodDisruptionBudgets()) != 0 {
		t.Fatal("PodDisruptionBudgets")
	}
	if len(view.Ingresses()) != 0 {
		t.Fatal("Ingresses")
	}
	if len(view.Nodes()) != 0 {
		t.Fatal("Nodes")
	}
}
