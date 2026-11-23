package onboarding

import (
	"context"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func ownedConfigMap(name string) *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "app-sdo", Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"},
	}}
}

// A from-scratch reset retires the install's start-up record with the rest of
// the control state; a reset for a restart of the same install keeps it and
// still retires everything else.
func TestResetControlStateKeepsTheInstallStartupOnlyForARestart(t *testing.T) {
	for _, keep := range []bool{false, true} {
		client := fake.NewSimpleClientset(ownedConfigMap(ConfigMapName), ownedConfigMap(InstallStartupConfigMapName))
		deleted, err := ResetControlState(context.Background(), client, "app-sdo", ResetControlStateOptions{KeepInstallStartup: keep})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(deleted, ConfigMapName) {
			t.Fatalf("keep=%v: onboarding record not retired: %v", keep, deleted)
		}
		_, err = client.CoreV1().ConfigMaps("app-sdo").Get(context.Background(), InstallStartupConfigMapName, metav1.GetOptions{})
		if kept := err == nil; kept != keep {
			t.Fatalf("keep=%v: install start-up record kept=%v (%v)", keep, kept, err)
		}
		if err != nil && !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
	}
}
