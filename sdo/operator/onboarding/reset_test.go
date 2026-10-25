package onboarding

import (
	"context"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestResetControlStateDeletesOnlyOwnedOnboarding(t *testing.T) {
	objects := []runtime.Object{&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "application", Namespace: "control"}}, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "control"}}}
	names := []string{ConfigMapName, PendingConfigMapName, AttemptConfigMapName}
	for _, name := range names {
		objects = append(objects, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "control", Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"}}})
	}
	client := fake.NewSimpleClientset(objects...)
	deleted, err := ResetControlState(context.Background(), client, "control")
	if err != nil || !reflect.DeepEqual(deleted, names) {
		t.Fatalf("reset: %v %v", deleted, err)
	}
	for _, action := range client.Actions() {
		if action.GetResource().Resource != "configmaps" {
			t.Fatalf("unexpected API access: %v", action)
		}
	}
	cms, err := client.CoreV1().ConfigMaps("control").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(cms.Items) != 1 || cms.Items[0].Name != "application" {
		t.Fatalf("application changed: %v %v", cms, err)
	}
	deleted, err = ResetControlState(context.Background(), client, "control")
	if err != nil || len(deleted) != 0 {
		t.Fatalf("idempotent reset: %v %v", deleted, err)
	}
}

func TestResetControlStateRefusesUnownedBeforeDeleting(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: "control", Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"}}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: AttemptConfigMapName, Namespace: "control"}})
	if _, err := ResetControlState(context.Background(), client, "control"); err == nil {
		t.Fatal("unowned name accepted")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			t.Fatal("partial deletion before ownership validation")
		}
	}
	if _, err := ResetControlState(context.Background(), client, ""); err == nil {
		t.Fatal("empty namespace accepted")
	}
}
