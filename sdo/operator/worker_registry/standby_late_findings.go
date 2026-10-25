package worker_registry

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/latefindings"
)

// The already-mounted read-only directory replaces only the ConfigMap volume
// projection; report identity, receipt format and controller ownership persist.
func (e KubernetesExecutor) projectWarmLateFindings(ctx context.Context) error {
	if e.WarmBinding == nil || e.LateFindingsConfigMap == "" {
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
		return fmt.Errorf("warm late findings identity mismatch")
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
