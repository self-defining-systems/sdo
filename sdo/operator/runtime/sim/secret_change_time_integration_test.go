//go:build integration

package sim_test

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/runtime/sim"
	"sdo.dev/operator/sdk"
)

// The API server stamps managedFields with wall-clock seconds; writes that
// must be told apart in time are separated by one.
func nextManagedFieldsSecond() { time.Sleep(1100 * time.Millisecond) }

func secretApply(value string) []byte {
	return []byte(`{"apiVersion":"v1","kind":"Secret","metadata":{"name":"token","namespace":"app"},"data":{"token":"` + base64.StdEncoding.EncodeToString([]byte(value)) + `"}}`)
}

func applySecret(t *testing.T, client kubernetes.Interface, manager, value string) {
	t.Helper()
	force := true
	if _, err := client.CoreV1().Secrets("app").Patch(context.Background(), "token", types.ApplyPatchType, secretApply(value), metav1.PatchOptions{FieldManager: manager, Force: &force}); err != nil {
		t.Fatal(err)
	}
}

func secretChangedAt(t *testing.T, client kubernetes.Interface) (time.Time, corev1.Secret) {
	t.Helper()
	secret, err := client.CoreV1().Secrets("app").Get(context.Background(), "token", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return sdk.SecretMetadataFrom(*secret).DataChangedAt, *secret
}

func foreignWrite(t *testing.T, client kubernetes.Interface, manager, value string) {
	t.Helper()
	secret, err := client.CoreV1().Secrets("app").Get(context.Background(), "token", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	secret.Data = map[string][]byte{"token": []byte(value)}
	if _, err := client.CoreV1().Secrets("app").Update(context.Background(), secret, metav1.UpdateOptions{FieldManager: manager}); err != nil {
		t.Fatal(err)
	}
}

// Against the real managedFields library: SDO adopting a Secret with the
// same data, re-applying it, a responder write and SDO reclaiming the data
// with force never move its data-change time; a foreign data write does.
func TestSecretDataChangeTimeIgnoresSDOWritesOnTheAPIServer(t *testing.T) {
	w := sim.NewWorld(1)
	client, err := w.Client()
	if err != nil {
		t.Fatal(err)
	}
	created := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "token"}, Data: map[string][]byte{"token": []byte("installed")}}
	if _, err := client.CoreV1().Secrets("app").Create(context.Background(), created, metav1.CreateOptions{FieldManager: "installer"}); err != nil {
		t.Fatal(err)
	}
	installed, _ := secretChangedAt(t, client)
	if installed.IsZero() {
		t.Fatal("a foreign create must set the data-change time")
	}

	nextManagedFieldsSecond()
	applySecret(t, client, sdk.DeployerFieldManager, "installed")
	if got, secret := secretChangedAt(t, client); !got.Equal(installed) {
		t.Fatalf("SDO adoption moved the data-change time to %s (installed %s); managedFields %+v", got, installed, secret.ManagedFields)
	}
	nextManagedFieldsSecond()
	applySecret(t, client, sdk.DeployerFieldManager, "installed")
	if got, _ := secretChangedAt(t, client); !got.Equal(installed) {
		t.Fatalf("a no-op re-apply moved the data-change time to %s", got)
	}
	nextManagedFieldsSecond()
	// A responder value change takes the key from every other manager, so no
	// foreign owner remains and the time is zero; it never becomes the
	// responder's write time.
	foreignWrite(t, client, sdk.ResponderFieldManager, "repaired")
	if got, _ := secretChangedAt(t, client); got.After(installed) {
		t.Fatalf("a responder write moved the data-change time to %s", got)
	}

	nextManagedFieldsSecond()
	foreignWrite(t, client, "rotator", "rotated")
	rotated, _ := secretChangedAt(t, client)
	if !rotated.After(installed) {
		t.Fatalf("a foreign data write left the data-change time at %s (installed %s)", rotated, installed)
	}

	nextManagedFieldsSecond()
	applySecret(t, client, sdk.DeployerFieldManager, "accepted")
	if got, secret := secretChangedAt(t, client); !got.IsZero() {
		t.Fatalf("after SDO force-reclaimed every data field the time is %s, want zero; managedFields %+v", got, secret.ManagedFields)
	}
}

// An object stored before any field ownership was recorded gains a
// before-first-apply entry stamped with SDO's first apply. That stamp is not
// a data change: the time stays the creation time.
func TestSecretAdoptedWithoutManagedFieldsKeepsItsCreationTime(t *testing.T) {
	w := sim.NewWorld(1)
	createdAt := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	w.Kube.Put(&unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Secret",
		"metadata": map[string]any{"name": "token", "namespace": "app", "creationTimestamp": createdAt.UTC().Format(time.RFC3339)},
		"data":     map[string]any{"token": "aW5zdGFsbGVk", "other": "b3RoZXI="},
	}})
	client, err := w.Client()
	if err != nil {
		t.Fatal(err)
	}
	applySecret(t, client, sdk.DeployerFieldManager, "installed")
	got, secret := secretChangedAt(t, client)
	adopted := false
	for _, entry := range secret.ManagedFields {
		adopted = adopted || entry.Manager == sdk.BeforeFirstApplyFieldManager
	}
	if !adopted {
		t.Fatalf("the API server recorded no %s entry: %+v", sdk.BeforeFirstApplyFieldManager, secret.ManagedFields)
	}
	if !got.Equal(createdAt.Time) {
		t.Fatalf("data-change time = %s, want the creation time %s", got, createdAt.Time)
	}
}
