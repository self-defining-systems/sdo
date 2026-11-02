package worker_registry

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/latefindings"
)

// projectLateFindings copies the controller-owned report ConfigMap into the
// session's read-only late-findings directory. Warm and cold workers read the
// same mount; a pod that predates its incident cannot mount a ConfigMap.
// A projection error degrades to no late findings for this poll; it never
// aborts the incident.
func (e KubernetesExecutor) projectLateFindings(ctx context.Context, task *contracts.AgentTask) {
	if err := e.writeLateFindings(ctx, task); err != nil {
		slog.Warn("late_findings_projection_failed", "task_id", task.GetTaskId(), "error", err)
	}
}

func (e KubernetesExecutor) writeLateFindings(ctx context.Context, task *contracts.AgentTask) error {
	if e.LateFindingsConfigMap == "" || e.SessionPVCSubPath == "" || !task.GetRequest().MessageIs(&contracts.IncidentRequest{}) {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	cm, err := e.Client.CoreV1().ConfigMaps(e.Namespace).Get(ctx, e.LateFindingsConfigMap, metav1.GetOptions{})
	if err != nil {
		return err
	}
	data := []byte(cm.Data[latefindings.ReportKey])
	report, err := latefindings.ParseReport(data)
	if err != nil {
		return err
	}
	if latefindings.ConfigMapName(report.GetIncidentId()) != e.LateFindingsConfigMap {
		return fmt.Errorf("late findings identity mismatch")
	}
	relative, err := filepath.Rel(sessionPVCBase(e.RepositoryPVCSubPath), e.SessionPVCSubPath)
	if err != nil {
		return err
	}
	directory := filepath.Join(e.RepositoryMountPath, relative, "late-findings")
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".report-")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporary.Name(), filepath.Join(directory, latefindings.ReportKey))
}
