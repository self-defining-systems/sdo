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
	return mark(ctx, client, control, WindowConfigMapName, "sampling started")
}
func Reject(ctx context.Context, client kubernetes.Interface, control, reason string) error {
	return mark(ctx, client, control, RejectedConfigMapName, reason)
}
func Marked(ctx context.Context, client kubernetes.Interface, control, name string) (bool, error) {
	_, err := client.CoreV1().ConfigMaps(control).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}
func mark(ctx context.Context, client kubernetes.Interface, control, name, reason string) error {
	immutable := true
	_, err := client.CoreV1().ConfigMaps(control).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: control, Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"}}, Immutable: &immutable, Data: map[string]string{"reason": reason}}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}
