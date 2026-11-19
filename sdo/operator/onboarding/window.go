package onboarding

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const WindowConfigMapName = "sdo-namespace-onboarding-window"
const RejectedConfigMapName = "sdo-namespace-onboarding-rejected"

// MarkWindow records that volatile sampling has begun. A new process cannot
// prove continuity of the goal/configuration observations and must fail closed.
func MarkWindow(ctx context.Context, client kubernetes.Interface, control string) error {
	return mark(ctx, client, control, WindowConfigMapName, map[string]string{"reason": "sampling started"})
}

// Reject records the durable rejection: its human reason and its stable cause (class and the check that held it,
// without timings or generated names), so a reader can tell a repeated rejection from a new one.
func Reject(ctx context.Context, client kubernetes.Interface, control, reason, cause string) error {
	return mark(ctx, client, control, RejectedConfigMapName, map[string]string{"reason": reason, "cause": cause})
}
func Marked(ctx context.Context, client kubernetes.Interface, control, name string) (bool, error) {
	_, err := client.CoreV1().ConfigMaps(control).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}
func mark(ctx context.Context, client kubernetes.Interface, control, name string, data map[string]string) error {
	immutable := true
	_, err := client.CoreV1().ConfigMaps(control).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: control, Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"}}, Immutable: &immutable, Data: data}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}
