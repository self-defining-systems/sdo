package install

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"sdo.dev/operator/operability"
)

// OperabilityConfigMap is the control-namespace ConfigMap that exposes the
// current operability report: "summary.json" (accepted commit, stage, digest,
// observation time, one line per item) and "report.json". It is current-state
// only; the broker's evidence store keeps the history.
const OperabilityConfigMap = "sdo-operability"

// OperabilityObligationsKey is the ConfigMap key holding the open upstream
// obligations (a JSON list with cause, owner and attempts), written by the
// controller's follower beside the report. PublishOperability keeps it.
const OperabilityObligationsKey = "obligations.json"

// PublishOperability creates or updates the sdo-operability ConfigMap in
// namespace from a report, with the same labels as the other SDO-managed
// ConfigMaps. The report is the broker's verified one; nothing here decides.
func PublishOperability(ctx context.Context, client kubernetes.Interface, namespace string, report operability.Report, observedAt time.Time) error {
	if client == nil {
		return fmt.Errorf("publish operability report: Kubernetes client is required")
	}
	data, err := operability.SummaryData(report, observedAt)
	if err != nil {
		return fmt.Errorf("publish operability report: %w", err)
	}
	configMaps := client.CoreV1().ConfigMaps(namespace)
	want := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: OperabilityConfigMap, Namespace: namespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"}}, Data: data}
	prior, err := configMaps.Get(ctx, OperabilityConfigMap, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		_, err = configMaps.Create(ctx, want, metav1.CreateOptions{})
	case err == nil:
		want.ResourceVersion = prior.ResourceVersion
		if obligations, ok := prior.Data[OperabilityObligationsKey]; ok {
			want.Data[OperabilityObligationsKey] = obligations
		}
		_, err = configMaps.Update(ctx, want, metav1.UpdateOptions{})
	}
	return err
}

// PublishOperabilityObligations sets the obligations summary in the existing
// sdo-operability ConfigMap, leaving the report keys as they are. Without a
// report there is nothing to attach it to and nothing is created: the
// sdo-upstream-obligations ConfigMap stays the durable record.
func PublishOperabilityObligations(ctx context.Context, client kubernetes.Interface, namespace, summary string) error {
	if client == nil {
		return fmt.Errorf("publish operability obligations: Kubernetes client is required")
	}
	configMaps := client.CoreV1().ConfigMaps(namespace)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		prior, err := configMaps.Get(ctx, OperabilityConfigMap, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		next := prior.DeepCopy()
		if next.Data == nil {
			next.Data = map[string]string{}
		}
		if next.Data[OperabilityObligationsKey] == summary {
			return nil
		}
		next.Data[OperabilityObligationsKey] = summary
		_, err = configMaps.Update(ctx, next, metav1.UpdateOptions{})
		return err
	})
}
