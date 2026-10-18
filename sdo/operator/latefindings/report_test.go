package latefindings

import (
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestControllerReportProjectsOnlyMatchingLateFindings(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	store := Store{Client: client, Namespace: "control"}
	if err := store.Ensure(ctx, "incident-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(ctx, Event{IncidentID: "incident-1", Relation: "after_dispatch", Event: "activated", DetectorID: "health", Fingerprint: "rule/demo", PlaybookIDs: []string{"repair"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(ctx, Event{IncidentID: "incident-2", Relation: "after_dispatch", Event: "activated", DetectorID: "other", Fingerprint: "other"}); err != nil {
		t.Fatal(err)
	}
	cm, err := client.CoreV1().ConfigMaps("control").Get(ctx, ConfigMapName("incident-1"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var report Report
	if err := json.Unmarshal([]byte(cm.Data[ReportKey]), &report); err != nil {
		t.Fatal(err)
	}
	if report.IncidentID != "incident-1" || len(report.LateFindings) != 1 || report.LateFindings[0].PlaybookIDs[0] != "repair" {
		t.Fatalf("bad report: %+v", report)
	}
	digest, err := Digest(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.VerifyPull(ctx, "incident-1", 1, digest, []string{"health"}, []string{"repair"}); err != nil {
		t.Fatalf("valid pull rejected: %v", err)
	}
	if err := store.VerifyPull(ctx, "incident-1", 1, digest, []string{"other"}, []string{"repair"}); err == nil {
		t.Fatal("forged pull detectors accepted")
	}
	if err := store.Update(ctx, Event{IncidentID: "incident-1", Event: "cleared", DetectorID: "health", Fingerprint: "rule/demo"}); err != nil {
		t.Fatal(err)
	}
	cm, err = client.CoreV1().ConfigMaps("control").Get(ctx, ConfigMapName("incident-1"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(cm.Data[ReportKey]), &report); err != nil {
		t.Fatal(err)
	}
	if report.LateFindings[0].StillActive {
		t.Fatal("clear did not update projected report")
	}
	_ = corev1.ConfigMap{}
}
