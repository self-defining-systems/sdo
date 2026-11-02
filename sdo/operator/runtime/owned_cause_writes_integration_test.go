//go:build integration

package runtime

import (
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestProgramReceiptRecordsOwnerChainFromPreImages(t *testing.T) {
	rs := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "social-network", Name: "home-timeline-service-68876fd868"}
	image := &unstructured.Unstructured{}
	image.SetAPIVersion("apps/v1")
	image.SetKind("ReplicaSet")
	image.SetNamespace(rs.Namespace)
	image.SetName(rs.Name)
	controller := true
	image.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "home-timeline-service", UID: "uid-1", Controller: &controller}})
	started := time.Now().UTC()
	applications := []RecordedPlanApplication{{ID: "application-1", ProgramIDs: []string{"rollout"}, StartedAt: started, CompletedAt: started.Add(time.Second), Result: PlanApplicationResult{
		Applied: true, ActionsApplied: []program.Action{program.SetField{Ref: rs, Path: "spec.replicas", Value: testPlanValue(0)}}, PreImages: []PlanPreImage{{Ref: rs, Object: image}},
	}}}
	receipts := ProgramRepairReceipts(applications, nil)
	want := []ResourceOwners{{Resource: rs, OwnerChain: []sdk.ObjectRef{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: rs.Namespace, Name: "home-timeline-service"}}}}
	if len(receipts) != 1 || !reflect.DeepEqual(receipts[0].Owners, want) {
		t.Fatalf("owner chain not recorded from the pre-image: %+v", receipts)
	}
	message, err := incidentResultContract(IncidentResult{RepairActions: receipts})
	if err != nil {
		t.Fatal(err)
	}
	if got := incidentResultState(message).RepairActions; len(got) != 1 || !reflect.DeepEqual(got[0].Owners, want) {
		t.Fatalf("owner chain lost at the contract boundary: %+v", message.GetRepairActions())
	}
}

