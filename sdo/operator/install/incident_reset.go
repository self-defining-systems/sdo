package install

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ResetConfigMap is the ConfigMap, in the control namespace, through which an
// operator (for example a benchmark harness between problems) asks the
// controller to reset: `requested` holds an opaque generation, and the
// controller writes the same value to `acknowledged` once the reset is
// durable.
const ResetConfigMap = "sdo-controller-reset"

const (
	resetRequestedKey    = "requested"
	resetAcknowledgedKey = "acknowledged"
)

// ResetRequests reads and writes the reset ConfigMap.
type ResetRequests struct {
	Client    kubernetes.Interface
	Namespace string
}

// Pending returns a requested generation not yet acknowledged, or "".
func (r ResetRequests) Pending(ctx context.Context) (string, error) {
	data, err := r.read(ctx)
	if err != nil {
		return "", err
	}
	requested := strings.TrimSpace(data[resetRequestedKey])
	if requested == strings.TrimSpace(data[resetAcknowledgedKey]) {
		return "", nil
	}
	return requested, nil
}

// Acknowledged returns the last acknowledged generation, empty when none.
func (r ResetRequests) Acknowledged(ctx context.Context) (string, error) {
	data, err := r.read(ctx)
	return strings.TrimSpace(data[resetAcknowledgedKey]), err
}

// Request records a reset request under generation.
func (r ResetRequests) Request(ctx context.Context, generation string) error {
	return r.update(ctx, func(data map[string]string) { data[resetRequestedKey] = generation })
}

// Acknowledge records that the controller completed the reset of generation.
func (r ResetRequests) Acknowledge(ctx context.Context, generation string) error {
	return r.update(ctx, func(data map[string]string) { data[resetAcknowledgedKey] = generation })
}

func (r ResetRequests) read(ctx context.Context) (map[string]string, error) {
	configMap, err := r.Client.CoreV1().ConfigMaps(r.Namespace).Get(ctx, ResetConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read reset ConfigMap %s/%s: %w", r.Namespace, ResetConfigMap, err)
	}
	return configMap.Data, nil
}

func (r ResetRequests) update(ctx context.Context, change func(map[string]string)) error {
	configMaps := r.Client.CoreV1().ConfigMaps(r.Namespace)
	configMap, err := configMaps.Get(ctx, ResetConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		configMap = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ResetConfigMap, Namespace: r.Namespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"}}, Data: map[string]string{}}
		change(configMap.Data)
		_, err = configMaps.Create(ctx, configMap, metav1.CreateOptions{})
	} else if err == nil {
		if configMap.Data == nil {
			configMap.Data = map[string]string{}
		}
		change(configMap.Data)
		_, err = configMaps.Update(ctx, configMap, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("update reset ConfigMap %s/%s: %w", r.Namespace, ResetConfigMap, err)
	}
	return nil
}
