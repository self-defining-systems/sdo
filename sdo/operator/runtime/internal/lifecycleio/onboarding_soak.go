package lifecycleio

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/journeycheck"
)

// OnboardingSoakStore reads and writes the install's one onboarding soak
// record (journeycheck.SoakConfigMap). The record is written once; it never
// changes afterwards. A from-scratch reinstall deletes it with the rest of the
// control state.
type OnboardingSoakStore struct {
	client    kubernetes.Interface
	namespace string
}

func NewOnboardingSoakStore(client kubernetes.Interface, namespace string) *OnboardingSoakStore {
	return &OnboardingSoakStore{client: client, namespace: namespace}
}

// Load returns the recorded soak, or nil when the install has none.
func (s *OnboardingSoakStore) Load(ctx context.Context) (*journeycheck.SoakRecord, error) {
	return LoadOnboardingSoak(ctx, s.client, s.namespace)
}

// LoadOnboardingSoak reads the soak record of the install whose control
// namespace is namespace; nil when there is none yet.
func LoadOnboardingSoak(ctx context.Context, client kubernetes.Interface, namespace string) (*journeycheck.SoakRecord, error) {
	configMap, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, journeycheck.SoakConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get onboarding soak ConfigMap: %w", err)
	}
	record, err := journeycheck.ParseSoakConfigMap(namespace, configMap.Data)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// Save records the soak. A record already present is kept: the install soaks
// once.
func (s *OnboardingSoakStore) Save(ctx context.Context, record journeycheck.SoakRecord) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode onboarding soak record: %w", err)
	}
	_, err = s.client.CoreV1().ConfigMaps(s.namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: journeycheck.SoakConfigMap, Namespace: s.namespace,
			Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"},
		},
		Data: map[string]string{journeycheck.SoakDataKey: string(payload)},
	}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("create onboarding soak ConfigMap: %w", err)
	}
	return nil
}
