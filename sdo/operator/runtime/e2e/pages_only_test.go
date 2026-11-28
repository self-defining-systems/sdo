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
// fault SDO's user tests see opens no incident; the harness page opens one,
// a scripted agent repairs it and the closing turn learns. The same fault
// again opens nothing until paged, and the page's incident is cleared by the
// deterministic stage with the learned playbook and no agent.
func TestPagesOnlyArmLearnsThenRepeatIsDeterministic(t *testing.T) {
	w := NewWorld(t)
	w.DeployToy()
	w.InstallController(func(policy *settings.Config) {
		policy.Detection.PagesOnly = true
		policy.Response.AfterPlaybook = settings.AfterPlaybookWait
		policy.Response.AfterPlaybookWait = settings.Duration(5 * time.Minute)
	})
	w.WaitBaseline(5 * time.Minute)
	const problem = "toy_target_port_frontend"

	for round, page := range []string{"page-1", "page-2"} {
		w.Inject(problem)
		// Longer than the toy user tests need to confirm a failure: nothing opens on its own.
		time.Sleep(45 * time.Second)
		if ids := w.OpenedIncidents(); len(ids) != round {
			t.Fatalf("round %d: a detector finding opened an incident under pages_only: %v", round+1, ids)
		}
		if w.Repaired(problem) {
			t.Fatalf("round %d: the fault cleared without a page", round+1)
		}
		paged := time.Now()
		w.Page(page)
		w.waitFor(time.Minute, page+" opens an incident", func() bool { return w.pageOpened(page) })
		w.waitFor(3*time.Minute, page+" repair", func() bool { return w.Repaired(problem) })
		t.Logf("round %d repaired %s after the page", round+1, time.Since(paged).Round(time.Second))
		w.waitFor(3*time.Minute, page+" outcome", func() bool { return len(w.Outcomes()) >= round+1 })
		if round == 0 {
			w.waitFor(3*time.Minute, "closing-turn activation", func() bool { return len(w.Activations()) >= 2 })
		}
	}
	ids := w.OpenedIncidents()
	if len(ids) != 2 {
		t.Fatalf("two pages must open two incidents, got %v", ids)
	}
	if o, _ := w.Incident(ids[0]); o.Raw["resolution_path"] != "responder" || o.Raw["classification"] != "success" {
		t.Errorf("first paged incident: want a successful responder repair, got path=%v class=%v", o.Raw["resolution_path"], o.Raw["classification"])
	}
	if o, _ := w.Incident(ids[1]); o.Raw["resolution_path"] != "deterministic" {
		t.Errorf("repeat paged incident must be cleared by the deterministic stage, got path=%v", o.Raw["resolution_path"])
	}
	if jobs := w.ResponderJobs(ids[1]); len(jobs) != 0 {
		t.Errorf("repeat paged incident must not start an agent, got Jobs %v", jobs)
	}
	w.Quiesce()
	w.settle(2 * time.Minute)
	for _, v := range w.CheckEveryIncidentEnds() {
		t.Errorf("property: %s", v)
	}
}
