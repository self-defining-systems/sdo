package worker_registry

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/latefindings"
)

// A cold incident Job reads late findings through the same read-only session
// directory a warm pod does; the controller projects the report before launch.
func TestColdIncidentProjectsLateFindingsIntoReadOnlySessionDirectory(t *testing.T) {
	_, e, task := standbyFixture(t)
	if err := (latefindings.Store{Client: e.Client, Namespace: e.Namespace}).Ensure(context.Background(), "incident"); err != nil {
		t.Fatal(err)
	}
	e.LateFindingsConfigMap = latefindings.ConfigMapName("incident")
	_, _ = executeUntil(t, jobExists(e, JobName(task.TaskId)), func(ctx context.Context) (*contracts.AgentResult, error) { return e.Execute(ctx, task) })
	if _, err := os.Stat(filepath.Join(e.RepositoryMountPath, "session/late-findings", latefindings.ReportKey)); err != nil {
		t.Fatal("report was not projected before the worker launched", err)
	}
	job, err := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), JobName(task.TaskId), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"/sdo/late-findings/report.json", "/sdo/session/late-findings/report.json"} {
		best := -1
		readOnly := false
		for _, m := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
			if strings.HasPrefix(target, m.MountPath+"/") && len(m.MountPath) > best {
				best, readOnly = len(m.MountPath), m.ReadOnly
			}
		}
		if best < 0 || !readOnly {
			t.Errorf("%s is not served by a read-only mount", target)
		}
	}
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.ConfigMap != nil && volume.ConfigMap.Name == e.LateFindingsConfigMap {
			t.Error("late findings still mounted as a ConfigMap volume")
		}
	}
}
