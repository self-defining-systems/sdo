package latefindings

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

func firing(incident, event, relation, detector, fingerprint string, playbooks ...string) *contracts.FiringRecord {
	return &contracts.FiringRecord{
		IncidentId: incident, Event: event, DispatchRelation: relation, DetectorId: detector,
		Fingerprint: fingerprint, SurfacedPlaybooks: playbooks, RecordedAt: timestamppb.New(time.Unix(1700000000, 0)),
	}
}

func readReport(t *testing.T, client *fake.Clientset, incident string) *contracts.LateFindingsReport {
	t.Helper()
	cm, err := client.CoreV1().ConfigMaps("control").Get(context.Background(), ConfigMapName(incident), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	report := &contracts.LateFindingsReport{}
	if err := wire.Unmarshal([]byte(cm.Data[ReportKey]), report); err != nil {
		t.Fatal(err)
	}
	return report
}

func TestControllerReportProjectsOnlyMatchingLateFindings(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	store := Store{Client: client, Namespace: "control"}
	if err := store.Ensure(ctx, "incident-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(ctx, firing("incident-1", "activated", "after_dispatch", "health", "rule/demo", "repair")); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(ctx, firing("incident-2", "activated", "after_dispatch", "other", "other")); err != nil {
		t.Fatal(err)
	}
	report := readReport(t, client, "incident-1")
	if report.GetIncidentId() != "incident-1" || len(report.GetLateFindings()) != 1 || report.GetLateFindings()[0].GetPlaybookIds()[0] != "repair" {
		t.Fatalf("bad report: %v", report)
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
	if err := store.Update(ctx, firing("incident-1", "cleared", "no_incident", "health", "rule/demo")); err != nil {
		t.Fatal(err)
	}
	if readReport(t, client, "incident-1").GetLateFindings()[0].GetStillActive() {
		t.Fatal("clear did not update projected report")
	}
}

func TestReportRejectsPhysicalPlaybookPaths(t *testing.T) {
	ctx := context.Background()
	store := Store{Client: fake.NewSimpleClientset(), Namespace: "control"}
	if err := store.Update(ctx, firing("incident-1", "activated", "after_dispatch", "health", "rule/demo", ".sdo/playbooks/repair/README.md")); err == nil {
		t.Fatal("physical playbook path was projected to the worker")
	}
}
