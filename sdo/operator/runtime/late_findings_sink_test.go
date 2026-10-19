package runtime

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/latefindings"
)

func TestLateFindingSinkPublishesOnlyIncidentLogicalData(t *testing.T) {
	client := fake.NewSimpleClientset()
	store := latefindings.Store{Client: client, Namespace: "control"}
	if err := store.Ensure(context.Background(), "incident-1"); err != nil {
		t.Fatal(err)
	}
	sink := lateFindingSink{Store: store}
	if err := sink.Record(&contracts.FiringRecord{Event: string(FiringActivated), IncidentId: "incident-1", DispatchRelation: string(RelationAfterDispatch), DetectorId: "detector", Fingerprint: "rule/x", SurfacedPlaybooks: []string{"repair"}, RecordedAt: timestamppb.New(time.Now())}); err != nil {
		t.Fatal(err)
	}
	cm, err := client.CoreV1().ConfigMaps("control").Get(context.Background(), latefindings.ConfigMapName("incident-1"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	report := &contracts.LateFindingsReport{}
	if err := wire.Unmarshal([]byte(cm.Data[latefindings.ReportKey]), report); err != nil {
		t.Fatal(err)
	}
	if len(report.GetLateFindings()) != 1 || report.GetLateFindings()[0].GetPlaybookIds()[0] != "repair" {
		t.Fatalf("incorrect projected report: %v", report)
	}
}
