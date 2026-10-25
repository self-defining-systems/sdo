package onboarding

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestResetControlStateDeletesOnlyOwnedOnboarding(t *testing.T) {
	objects := []runtime.Object{&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "application", Namespace: "control"}}, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "control"}}}
	names := []string{ConfigMapName, PendingConfigMapName, AttemptConfigMapName}
	for _, name := range names {
		objects = append(objects, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "control", UID: types.UID(name), ResourceVersion: "7", Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"}}})
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
		if deletion, ok := action.(ktesting.DeleteAction); ok {
			preconditions := deletion.GetDeleteOptions().Preconditions
			if preconditions == nil || preconditions.UID == nil || *preconditions.UID != types.UID(deletion.GetName()) || preconditions.ResourceVersion == nil || *preconditions.ResourceVersion != "7" {
				t.Fatalf("deletion lost identity preconditions: %+v", preconditions)
			}
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

func TestResetControlStateRetryAfterPartialFailure(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	for _, name := range []string{ConfigMapName, PendingConfigMapName, AttemptConfigMapName} {
		_, err := client.CoreV1().ConfigMaps("control").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"}}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
	failed := false
	client.PrependReactor("delete", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.(ktesting.DeleteAction).GetName() == PendingConfigMapName && !failed {
			failed = true
			return true, nil, fmt.Errorf("injected deletion failure")
		}
		return false, nil, nil
	})
	deleted, err := ResetControlState(ctx, client, "control")
	if err == nil || !reflect.DeepEqual(deleted, []string{ConfigMapName}) {
		t.Fatalf("partial audit: %v %v", deleted, err)
	}
	deleted, err = ResetControlState(ctx, client, "control")
	if err != nil || !reflect.DeepEqual(deleted, []string{PendingConfigMapName, AttemptConfigMapName}) {
		t.Fatalf("retry: %v %v", deleted, err)
	}
}

func TestResetControlStateRetiresLegacyControllerCheckpoint(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "sdo-controller-state", Namespace: "control", Annotations: map[string]string{"sdo.dev/state-revision": "4"}}, Data: map[string]string{"runtime-state.json": "{}"}})
	deleted, err := ResetControlState(ctx, client, "control")
	if err != nil || !reflect.DeepEqual(deleted, []string{"sdo-controller-state"}) {
		t.Fatalf("old checkpoint can resurrect onboarding: %v %v", deleted, err)
	}
}

func TestResetControlStateRefusesUnownedCheckpoint(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "sdo-controller-state", Namespace: "control"}, Data: map[string]string{"runtime-state.json": "{}"}})
	if _, err := ResetControlState(context.Background(), client, "control"); err == nil {
		t.Fatal("a checkpoint name/data key without ownership was deleted")
	}
}
