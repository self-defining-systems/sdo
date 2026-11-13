package lifecycleio

import (
	"context"
	"errors"
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

// ErrInvalidAlert marks inbox content the contract refuses; the caller skips
// it instead of stopping.
var ErrInvalidAlert = errors.New("external incident inbox holds an invalid alert")

// ReadExternalIncident is a single-slot Kubernetes alert inbox. The producer
// retains the stable alert until controller acknowledgment, then may replace it.
// The controller's persisted incident identity makes delivery restart-safe.
func ReadExternalIncident(ctx context.Context, client kubernetes.Interface, namespace string) (*contracts.ExternalIncident, error) {
	cm, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, "sdo-external-incident", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	alert := &contracts.ExternalIncident{}
	if err := wire.Unmarshal([]byte(cm.Data["incident.json"]), alert); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidAlert, err)
	}
	return alert, nil
}
