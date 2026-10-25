package onboarding

import (
	"context"
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/servicehealth"
)

const ReachabilityDataKey = "reachability-reference.json"

// CompleteWithReachability atomically publishes identities and their confirmed
// controller-owned reference. The immutable record is never expanded.
func CompleteWithReachability(ctx context.Context, client kubernetes.Interface, control string, record *contracts.NamespaceOnboarding, reference *servicehealth.ReachabilityBaseline) (*contracts.NamespaceOnboarding, error) {
	if reference == nil {
		return Complete(ctx, client, control, record)
	}
	if err := reference.Validate(); err != nil {
		return nil, err
	}
	if err := Validate(record, record.GetNamespace()); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(reference)
	if err != nil {
		return nil, err
	}
	return persistData(ctx, client, control, ConfigMapName, record, map[string]string{ReachabilityDataKey: string(raw)})
}

// LoadReachability consumes only the finalized immutable install record.
func LoadReachability(ctx context.Context, client kubernetes.Interface, control, namespace string) (*servicehealth.ReachabilityBaseline, error) {
	record, err := Load(ctx, client, control, namespace)
	if err != nil || record == nil {
		return nil, err
	}
	cm, err := client.CoreV1().ConfigMaps(control).Get(ctx, ConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	raw := cm.Data[ReachabilityDataKey]
	if raw == "" {
		return nil, nil
	}
	var reference servicehealth.ReachabilityBaseline
	if err := json.Unmarshal([]byte(raw), &reference); err != nil {
		return nil, fmt.Errorf("decode onboarding reachability: %w", err)
	}
	if err := reference.Validate(); err != nil {
		return nil, err
	}
	return &reference, nil
}
