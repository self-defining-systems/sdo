package onboarding

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestVerifiedOnboardingIsCreateOnceAndNeverReadsSecrets(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "installer-script"}, Data: map[string]string{"script": "content-canary-not-an-identity"}}, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "query"}})
	client.PrependReactor("*", "secrets", func(ktesting.Action) (bool, runtime.Object, error) {
		t.Fatal("Secret API must never be accessed")
		return true, nil, nil
	})
	source := []*unstructured.Unstructured{{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"namespace": "app", "name": "query"}}}}
	first, err := Establish(ctx, client, "control", "app", strings.Repeat("a", 40), source, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(first.GetObjects()) != 1 || first.Objects[0].Name != "installer-script" {
		t.Fatalf("not out-of-source identities: %+v", first)
	}
	_, err = client.CoreV1().ConfigMaps("app").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "later-fault"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := client.CoreV1().ConfigMaps("control").Get(ctx, ConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stored.Immutable == nil || !*stored.Immutable || strings.Contains(stored.Data[DataKey], "content-canary") {
		t.Fatal("acknowledgement must be immutable and contain identities only")
	}
	later, err := Establish(ctx, client, "control", "app", strings.Repeat("b", 40), source, time.Unix(200, 0))
	if err != nil {
		t.Fatal(err)
	}
	if later.AcceptedCommit != first.AcceptedCommit || len(later.Objects) != 1 || !later.EstablishedAt.AsTime().Equal(first.EstablishedAt.AsTime()) {
		t.Fatalf("reinstall acknowledged a later fault: %+v", later)
	}
	loaded, err := Load(ctx, client, "control", "app")
	if err != nil || loaded.AcceptedCommit != first.AcceptedCommit {
		t.Fatalf("restart lost acknowledgement: %+v %v", loaded, err)
	}
}
