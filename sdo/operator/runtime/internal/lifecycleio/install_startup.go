package lifecycleio

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

const (
	// InstallStartupConfigMap holds the one install-scoped fact that outlives
	// retired controller state: the install's start-up period was established.
	// A restart of the same install whose controller state was retired (a
	// restart from memory) continues it instead of starting a new period. A
	// from-scratch reinstall deletes it with the rest of the control state.
	InstallStartupConfigMap = "sdo-install-startup"
	installStartupDataKey   = "startup.json"
)

// InstallStartupStore reads and writes the install's established start-up
// period. The record is written once; it never changes afterwards.
type InstallStartupStore struct {
	client    kubernetes.Interface
	namespace string
}

func NewInstallStartupStore(client kubernetes.Interface, namespace string) *InstallStartupStore {
	return &InstallStartupStore{client: client, namespace: namespace}
}

// Load returns the recorded period, or nil when the install has none.
func (s *InstallStartupStore) Load(ctx context.Context) (*contracts.StartupPeriod, error) {
	configMap, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, InstallStartupConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get install start-up ConfigMap: %w", err)
	}
	payload, ok := configMap.Data[installStartupDataKey]
	if !ok {
		return nil, fmt.Errorf("install start-up ConfigMap %s/%s has no %s", s.namespace, InstallStartupConfigMap, installStartupDataKey)
	}
	period := &contracts.StartupPeriod{}
	if err := wire.UnmarshalDurable([]byte(payload), period); err != nil {
		return nil, fmt.Errorf("decode install start-up period: %w", err)
	}
	if period.GetEstablishedAt() == nil {
		return nil, fmt.Errorf("install start-up ConfigMap %s/%s records no established time", s.namespace, InstallStartupConfigMap)
	}
	return period, nil
}

// Save records an established period. A record already present is kept: the
// first establishment of the install is the fact.
func (s *InstallStartupStore) Save(ctx context.Context, period *contracts.StartupPeriod) error {
	if period.GetEstablishedAt() == nil {
		return fmt.Errorf("install start-up record needs an established period")
	}
	payload, err := wire.Canonical(&contracts.StartupPeriod{BeganAt: period.GetBeganAt(), BoundAt: period.GetBoundAt(), EstablishedAt: period.GetEstablishedAt()})
	if err != nil {
		return fmt.Errorf("encode install start-up period: %w", err)
	}
	_, err = s.client.CoreV1().ConfigMaps(s.namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: InstallStartupConfigMap, Namespace: s.namespace,
			Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"},
		},
		Data: map[string]string{installStartupDataKey: string(payload)},
	}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("create install start-up ConfigMap: %w", err)
	}
	return nil
}
