package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/latefindings"
)

func TestLateFindingSinkPublishesOnlyIncidentLogicalData(t *testing.T) {
	client := fake.NewSimpleClientset()
	store := latefindings.Store{Client: client, Namespace: "control"}
	if err := store.Ensure(context.Background(), "incident-1"); err != nil {
		t.Fatal(err)
	}
	sink := lateFindingSink{Store: store}
	if err := sink.Record(FiringRecord{Event: FiringActivated, IncidentID: "incident-1", DispatchRelation: RelationAfterDispatch, DetectorID: "detector", Fingerprint: "rule/x", SurfacedPlaybooks: []string{".sdo/playbooks/repair/README.md"}, RecordedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	cm, err := client.CoreV1().ConfigMaps("control").Get(context.Background(), latefindings.ConfigMapName("incident-1"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var report latefindings.Report
	if err := json.Unmarshal([]byte(cm.Data[latefindings.ReportKey]), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.LateFindings) != 1 || report.LateFindings[0].PlaybookIDs[0] != "repair" {
		t.Fatalf("incorrect projected report: %+v", report)
	}
}
