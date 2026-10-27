package worker_registry

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/latefindings"
)

// The worker registry names every ConfigMap that belongs to a worker task. This
// file is the one list of those names; reset code asks it instead of copying
// prefixes.
const (
	jobNamePrefix           = "sdo-"
	incidentExecutionPrefix = "sdo-exec-"
	standbyJobPrefix        = "sdo-standby-"
)

// Per-Job ConfigMaps are the Job name plus one of these suffixes.
var workerJobSuffixes = []string{"-request", "-result", "-execution", "-binding"}

// IsWorkerConfigMap reports whether name is durable state of a worker task: a
// Job's request, result, dispatch receipt or standby binding, an incident
// execution identity, or a late-findings report. Controller, onboarding and
// registration ConfigMaps are never worker state.
func IsWorkerConfigMap(name string) bool {
	if strings.HasPrefix(name, incidentExecutionPrefix) || strings.HasPrefix(name, latefindings.NamePrefix) {
		return true
	}
	if !strings.HasPrefix(name, jobNamePrefix) {
		return false
	}
	for _, suffix := range workerJobSuffixes {
		if strings.HasSuffix(name, suffix) && len(name) > len(jobNamePrefix)+len(suffix) {
			return true
		}
	}
	return false
}

// ResetWorkerState deletes every worker ConfigMap in the control namespace and
// returns their names. The caller must have stopped the controller and every
// worker: a resumed task whose Job is gone would otherwise be dispatched from
// this stale state. Secrets are never read.
func ResetWorkerState(ctx context.Context, client kubernetes.Interface, control string) ([]string, error) {
	deleted := []string{}
	if control == "" {
		return deleted, fmt.Errorf("reset worker state requires an explicit control namespace")
	}
	maps, err := client.CoreV1().ConfigMaps(control).List(ctx, metav1.ListOptions{})
	if err != nil {
		return deleted, fmt.Errorf("list ConfigMaps in %s: %w", control, err)
	}
	for _, cm := range maps.Items {
		if !IsWorkerConfigMap(cm.Name) {
			continue
		}
		uid, version := cm.UID, cm.ResourceVersion
		err := client.CoreV1().ConfigMaps(control).Delete(ctx, cm.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return deleted, fmt.Errorf("delete worker ConfigMap %s/%s: %w", control, cm.Name, err)
		}
		deleted = append(deleted, cm.Name)
		slog.InfoContext(ctx, "reset SDO worker state", "kind", "ConfigMap", "namespace", control, "name", cm.Name, "uid", cm.UID)
	}
	return deleted, nil
}
