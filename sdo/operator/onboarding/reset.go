package onboarding

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ResetControlState retires namespace-lifetime onboarding authority before a
// from-scratch reinstall. The caller must stop the controller and its workers
// first. Application objects, Secrets, and repository memory are never accessed.
func ResetControlState(ctx context.Context, client kubernetes.Interface, control string) ([]string, error) {
	deleted := []string{}
	if control == "" {
		return deleted, fmt.Errorf("reset control state requires an explicit control namespace")
	}
	var records []*corev1.ConfigMap
	for _, name := range []string{ConfigMapName, PendingConfigMapName, AttemptConfigMapName, WindowConfigMapName, RejectedConfigMapName, "sdo-controller-state", "sdo-controller-maintenance"} {
		cm, err := client.CoreV1().ConfigMaps(control).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return deleted, fmt.Errorf("inspect ConfigMap %s/%s: %w", control, name, err)
		}
		owned := cm.Labels["app.kubernetes.io/managed-by"] == "sdo"
		// The legacy runtime storage edge predates managed-by labels. Its
		// exact checkpoint name, CAS annotation and typed data key jointly
		// identify SDO state; names alone never authorize deletion.
		if name == "sdo-controller-state" {
			_, hasState := cm.Data["runtime-state.json"]
			owned = owned || cm.Annotations["sdo.dev/state-revision"] != "" && hasState
		}
		if name == "sdo-controller-maintenance" && cm.Labels["app.kubernetes.io/managed-by"] == "" {
			// Older production SetMaintenance writes lacked an ownership
			// label. Only its exact closed wire shape identifies legacy state.
			owned = legacyMaintenanceRecord(cm)
		}
		if !owned {
			return deleted, fmt.Errorf("refuse unowned ConfigMap %s/%s", control, name)
		}
		records = append(records, cm)
	}
	for _, cm := range records {
		uid, version := cm.UID, cm.ResourceVersion
		err := client.CoreV1().ConfigMaps(control).Delete(ctx, cm.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return deleted, fmt.Errorf("delete ConfigMap %s/%s: %w", control, cm.Name, err)
		}
		deleted = append(deleted, cm.Name)
		slog.InfoContext(ctx, "reset SDO namespace control state", "kind", "ConfigMap", "namespace", control, "name", cm.Name, "uid", cm.UID, "resource_version", cm.ResourceVersion)
	}
	return deleted, nil
}

func legacyMaintenanceRecord(cm *corev1.ConfigMap) bool {
	if len(cm.BinaryData) != 0 || len(cm.Data) < 2 || len(cm.Data) > 3 {
		return false
	}
	if cm.Data["state"] != "active" && cm.Data["state"] != "paused" {
		return false
	}
	if strings.TrimSpace(cm.Data["generation"]) == "" {
		return false
	}
	for key, value := range cm.Data {
		switch key {
		case "state", "generation":
		case "preserve_baseline":
			if value != "true" && value != "false" {
				return false
			}
		default:
			return false
		}
	}
	return true
}
