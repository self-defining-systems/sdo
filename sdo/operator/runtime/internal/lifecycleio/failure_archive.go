package lifecycleio

import (
	"context"
	"crypto/sha256"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

const FailedClosureDataKey = "failed-closure.json"

func failedClosureName(payload []byte) string {
	return fmt.Sprintf("sdo-failed-closure-%x", sha256.Sum256(payload))[:63]
}

// archiveFailures is the outbox's storage edge. A successful Save guarantees
// each record is separately durable before the controller advances its state.
// The main ConfigMap never accumulates unbounded incident history.
func (s *ConfigMapStateStore) archiveFailures(ctx context.Context, records []*contracts.FailedIncidentClosure) error {
	configMaps := s.client.CoreV1().ConfigMaps(s.namespace)
	for _, record := range records {
		payload, err := wire.Canonical(record)
		if err != nil {
			return fmt.Errorf("encode failed closure: %w", err)
		}
		if err := checkBudget("failed closure archive", payload, record); err != nil {
			return err
		}
		id := record.GetFailure().GetIncidentId()
		if id == "" || record.GetClosure().GetRequest().GetIncidentId() != id {
			return fmt.Errorf("failed closure archive incident mismatch")
		}
		name := failedClosureName(payload)
		_, err = configMaps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.namespace, Labels: map[string]string{"sdo.dev/closure-archive": "failed"}}, Data: map[string]string{FailedClosureDataKey: string(payload)}}, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			prior, getErr := configMaps.Get(ctx, name, metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("read failed closure archive: %w", getErr)
			}
			if prior.Data[FailedClosureDataKey] != string(payload) {
				return fmt.Errorf("failed closure archive for %q has different evidence", id)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("archive failed closure %q: %w", id, err)
		}
	}
	return nil
}
