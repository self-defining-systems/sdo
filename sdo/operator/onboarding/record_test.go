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
	err := Stage(ctx, client, "control", "app", strings.Repeat("a", 40), source, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	pending, err := Pending(ctx, client, "control", "app")
	if err != nil {
		t.Fatal(err)
	}
	if claimed, err := Claim(ctx, client, "control"); err != nil || !claimed {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	first, err := Complete(ctx, client, "control", pending)
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
	err = Stage(ctx, client, "control", "app", strings.Repeat("b", 40), source, time.Unix(200, 0))
	if err != nil {
		t.Fatal(err)
	}
	later, err := Load(ctx, client, "control", "app")
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

func TestPendingOnboardingNeverRecapturesAndClaimSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "initial", Namespace: "app"}})
	if err := Stage(ctx, client, "control", "app", strings.Repeat("a", 40), nil, time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().ConfigMaps("app").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "later", Namespace: "app"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := Stage(ctx, client, "control", "app", strings.Repeat("b", 40), nil, time.Unix(200, 0)); err != nil {
		t.Fatal(err)
	}
	pending, err := Pending(ctx, client, "control", "app")
	if err != nil || pending.GetAcceptedCommit() != strings.Repeat("a", 40) || len(pending.GetObjects()) != 1 || pending.Objects[0].Name != "initial" {
		t.Fatalf("pending install snapshot expanded: %+v %v", pending, err)
	}
	if claimed, err := Claim(ctx, client, "control"); err != nil || !claimed {
		t.Fatalf("first claim: %v %v", claimed, err)
	}
	if claimed, err := Claim(ctx, client, "control"); err != nil || claimed {
		t.Fatalf("repeated claim after restart: %v %v", claimed, err)
	}
	if record, err := Load(ctx, client, "control", "app"); err != nil || record != nil {
		t.Fatalf("claim alone granted authority: %v %v", record, err)
	}
}

func TestInstallEnvironmentDigestBindsIncarnationAndConfiguration(t *testing.T) {
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
		"metadata": map[string]any{"name": "environment-role", "uid": "installed"},
		"rules":    []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"configmaps"}, "verbs": []any{"get"}}},
	}}
	before := EnvironmentDigest(object)
	bookkeeping := object.DeepCopy()
	bookkeeping.SetResourceVersion("999")
	bookkeeping.SetGeneration(9)
	bookkeeping.SetCreationTimestamp(metav1.NewTime(time.Unix(999, 0)))
	bookkeeping.Object["status"] = map[string]any{"observed": "new status"}
	if EnvironmentDigest(bookkeeping) != before {
		t.Fatal("API bookkeeping changed the install observation")
	}
	for _, change := range []string{"uid", "rules", "owner", "labels", "annotations"} {
		t.Run(change, func(t *testing.T) {
			changed := object.DeepCopy()
			switch change {
			case "uid":
				changed.SetUID("replacement")
			case "rules":
				changed.Object["rules"] = []any{map[string]any{"verbs": []any{"delete"}}}
			case "owner":
				changed.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "v1", Kind: "ServiceAccount", Name: "owner", UID: "owner"}})
			case "labels":
				changed.SetLabels(map[string]string{"owner": "changed"})
			case "annotations":
				changed.SetAnnotations(map[string]string{"configuration": "changed"})
			}
			if EnvironmentDigest(changed) == before {
				t.Fatal("changed environment inherited install observation")
			}
		})
	}
}
