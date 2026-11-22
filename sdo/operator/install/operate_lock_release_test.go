package install

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// The lock ConfigMap is owned by the sync Pod, so the cluster's garbage collector deletes it once the Pod is gone. A store that
// answers a delete of an already-removed object with a precondition Conflict (a vcluster's datastore did: "Precondition
// failed: ResourceVersion in precondition: 2196, ... in object meta: 2268") must not fail the release: an object that is gone,
// or replaced by another reservation, is released.
func TestReleasingTheOperateLockSurvivesAConflictFromAnObjectThatIsAlreadyGone(t *testing.T) {
	for name, release := range map[string]func(Installer, Config, *corev1.ConfigMap) error{
		"reservation": func(i Installer, cfg Config, lock *corev1.ConfigMap) error {
			return i.releaseOperateReservation(context.Background(), cfg, lock)
		},
		"operation": func(i Installer, cfg Config, lock *corev1.ConfigMap) error {
			return i.releaseOperateLock(context.Background(), cfg, lock.Data["operation_id"])
		},
	} {
		t.Run(name, func(t *testing.T) {
			lock := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: OperateLockConfigMap, Namespace: "ns", UID: "u1", ResourceVersion: "5"},
				Data: map[string]string{"operation_id": "op-1"}}
			client := fake.NewSimpleClientset(lock.DeepCopy())
			client.PrependReactor("delete", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
				// the garbage collector got there first: the object is gone, and the store reports a precondition conflict
				_ = client.Tracker().Delete(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, "ns", OperateLockConfigMap)
				return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, OperateLockConfigMap, nil)
			})
			if err := release(Installer{Client: client}, Config{Namespace: "ns"}, lock); err != nil {
				t.Fatalf("release failed on a lock that is already gone: %v", err)
			}
		})
	}
}
