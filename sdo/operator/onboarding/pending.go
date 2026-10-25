package onboarding

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const PendingConfigMapName = "sdo-namespace-onboarding-pending"
const AttemptConfigMapName = "sdo-namespace-onboarding-attempt"

// Stage freezes identities at install time without probing or granting authority.
// Reinstallation cannot replace the snapshot, including after a failed attempt.
func Stage(ctx context.Context, client kubernetes.Interface, control, namespace, commit string, source []*unstructured.Unstructured, now time.Time) error {
	if prior, err := Load(ctx, client, control, namespace); err != nil || prior != nil {
		return err
	}
	if prior, err := Pending(ctx, client, control, namespace); err != nil || prior != nil {
		return err
	}
	record, err := capture(ctx, client, namespace, commit, source, now)
	if err != nil {
		return err
	}
	_, err = persist(ctx, client, control, PendingConfigMapName, record)
	return err
}

func Pending(ctx context.Context, client kubernetes.Interface, control, namespace string) (*contracts.NamespaceOnboarding, error) {
	return load(ctx, client, control, PendingConfigMapName, namespace)
}

// Claim consumes the sole evaluation opportunity before detector execution.
// A crash or non-clear first evaluation leaves pending evidence without granting
// authority. Later healthy periods and restarts cannot retry it.
func Claim(ctx context.Context, client kubernetes.Interface, control string) (bool, error) {
	immutable := true
	_, err := client.CoreV1().ConfigMaps(control).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: AttemptConfigMapName, Namespace: control, Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"}}, Immutable: &immutable}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return false, nil
	}
	return err == nil, err
}

// Complete publishes the frozen install identities after the controller's clear
// first evaluation. It never samples the current inventory.
func Complete(ctx context.Context, client kubernetes.Interface, control string, record *contracts.NamespaceOnboarding) (*contracts.NamespaceOnboarding, error) {
	if err := Validate(record, record.GetNamespace()); err != nil {
		return nil, err
	}
	return persist(ctx, client, control, ConfigMapName, record)
}
