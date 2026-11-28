//go:build integration && controller_e2e

package e2e

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/contracts/settings"
)

// pagesOnlyNotice is the generic page: application and namespace, no finding.
const pagesOnlyNotice = "Application: toy\nNamespace: " + AppNamespace + "\n\nA small synthetic three-service application."

// Page writes the external-incident inbox the way the benchmark harness pages SDO.
func (w *World) Page(id string) {
	w.T.Helper()
	alert, err := json.Marshal(map[string]string{"id": id, "namespace": AppNamespace, "message": pagesOnlyNotice})
	if err != nil {
		w.T.Fatal(err)
	}
	ctx := context.Background()
	configMaps := w.Cluster.Client.CoreV1().ConfigMaps(ControlNamespace)
	inbox := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "sdo-external-incident", Namespace: ControlNamespace},
		Data: map[string]string{"incident.json": string(alert)}}
	if _, err := configMaps.Update(ctx, inbox, metav1.UpdateOptions{}); err != nil {
		if _, err := configMaps.Create(ctx, inbox, metav1.CreateOptions{}); err != nil {
			w.T.Fatal(err)
		}
	}
}

// pageOpened reports whether the controller opened an incident on the page.
func (w *World) pageOpened(id string) bool {
	for _, line := range w.ControllerLines() {
		if strings.Contains(line.Text, "page_opened") && strings.Contains(line.Text, "page_id="+id) {
			return true
		}
	}
	return false
}

// The detector-ablation arm (detection.pages_only, after_playbook wait): a
// fault SDO's user tests see opens no incident; the harness page opens one and
// a scripted agent repairs it. (The repeat with a learned playbook is covered
// by runtime.TestPagesOnlyPagedIncidentAppliesTheLearnedPlaybook: on this pin
// the hermetic world's scripted reflection fails its memory check, as in
// scenario 1, so no playbook is learned here.)
func TestPagesOnlyArmOpensOnlyOnAPage(t *testing.T) {
	w := NewWorld(t)
	w.DeployToy()
	w.InstallController(func(policy *settings.Config) {
		policy.Detection.PagesOnly = true
		policy.Response.AfterPlaybook = settings.AfterPlaybookWait
		policy.Response.AfterPlaybookWait = settings.Duration(5 * time.Minute)
	})
	w.WaitBaseline(5 * time.Minute)
	const problem = "toy_target_port_frontend"

	w.Inject(problem)
	// Longer than the toy user tests need to confirm a failure: nothing opens on its own.
	time.Sleep(45 * time.Second)
	if ids := w.OpenedIncidents(); len(ids) != 0 {
		t.Fatalf("a detector finding opened an incident under pages_only: %v", ids)
	}
	if w.Repaired(problem) {
		t.Fatal("the fault cleared without a page")
	}
	paged := time.Now()
	w.Page("page-1")
	w.waitFor(time.Minute, "the page opens an incident", func() bool { return w.pageOpened("page-1") })
	w.waitFor(3*time.Minute, "repair", func() bool { return w.Repaired(problem) })
	t.Logf("repaired %s after the page", time.Since(paged).Round(time.Second))
	w.waitFor(3*time.Minute, "outcome", func() bool { return len(w.Outcomes()) >= 1 })
	ids := w.OpenedIncidents()
	if len(ids) != 1 {
		t.Fatalf("one page must open one incident, got %v", ids)
	}
	o, _ := w.Incident(ids[0])
	t.Logf("outcome: path=%v class=%v", o.Raw["resolution_path"], o.Raw["classification"])
	if o.Raw["resolution_path"] != "responder" || o.Raw["classification"] != "success" {
		t.Errorf("paged incident: want a successful responder repair, got path=%v class=%v", o.Raw["resolution_path"], o.Raw["classification"])
	}
	w.Quiesce()
	w.settle(2 * time.Minute)
	for _, v := range w.CheckEveryIncidentEnds() {
		t.Errorf("property: %s", v)
	}
}
