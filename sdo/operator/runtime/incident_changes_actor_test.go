package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestChangeActorAttributesEachWriter(t *testing.T) {
	for writer, want := range map[string]string{
		sdk.ResponderFieldManager: ActorAgent, sdk.DeployerFieldManager: ActorSDO, sdk.AcceptanceFieldManager: ActorSDO,
		"kube-controller-manager": ActorKubernetes, "kubelet": ActorKubernetes, "deployment controller": ActorKubernetes,
		"kubectl-rollout": ActorOther, "helm": ActorOther, "": ActorUnknown, "?": ActorUnknown,
	} {
		if got := changeActor(writer); got != want {
			t.Errorf("changeActor(%q) = %q, want %q", writer, got, want)
		}
	}
}

// `sdo changes` attributes every row (departures by writer are covered by
// TestIncidentChangesListDeparturesWithWriterInTheWindow): the agent's recorded writes and its
// `sdo mitigate` calls, SDO's own stage, Kubernetes rollouts and anyone else.
func TestChangesListTheAgentsSDOsAndKubernetesChangesWithTheirActor(t *testing.T) {
	view := departureView{detectFake: detectApp("create"), diffs: map[string]sdk.SourceDiff{"Deployment/front": {Changed: true}}}
	onset := detectNow().Add(-time.Minute)
	view.ReplicaSetList = []appsv1.ReplicaSet{{ObjectMeta: metav1.ObjectMeta{Name: "front-7c9", Namespace: "app", CreationTimestamp: metav1.NewTime(onset.Add(-20 * time.Second)),
		Annotations:     map[string]string{"deployment.kubernetes.io/revision": "7"},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "front", Controller: boolPtr(true)}}}}}
	request := detectRequest()
	request.DetectedAt = onset
	request.DriftEvents = []sdk.DriftEvent{departureEvent(dref("Deployment", "front"), "kubectl-rollout", onset.Add(-21*time.Second), "spec.template.metadata.annotations")}
	loop := NewResponderLoop(request, nil, staticProvider{view}, nil, PlanExecutor{}, 10)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView { return program.ReadFrom(v) }
	loop.Now = detectNow
	loop.ExternalReceipts = func() ([]RepairActionReceipt, bool, string, error) {
		return []RepairActionReceipt{{Kind: "api-patch", Target: "Deployment/app/api", Resources: []sdk.ObjectRef{{Kind: "Deployment", Namespace: "app", Name: "api"}},
			Summary: "patched Deployment/api", Success: true, CompletedAt: onset.Add(10 * time.Second)}}, true, "", nil
	}
	loop.applicationsLog = []RecordedPlanApplication{
		{ID: "attempt-1-r1", ProgramIDs: []string{"restart-api"}, CompletedAt: onset.Add(20 * time.Second),
			Result: PlanApplicationResult{Applied: true, ObjectEffects: []PlanObjectEffect{{Ref: sdk.ObjectRef{Kind: "Pod", Namespace: "app", Name: "api-0"}, Confirmed: true, State: "applied"}}}},
		{ID: "application-s-1", ProgramIDs: []string{"scale-db"}, CompletedAt: onset.Add(5 * time.Second),
			Result: PlanApplicationResult{Applied: true, ObjectEffects: []PlanObjectEffect{{Ref: sdk.ObjectRef{Kind: "Pod", Namespace: "app", Name: "db-0"}, Confirmed: true, State: "applied"}}}},
	}
	response := loop.Handle(context.Background(), ToolRequest{Command: "changes"})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	rows := parseCSV(t, []byte(response.Text))
	actors := map[string]string{}
	for _, row := range rows[1:] {
		actors[row[2]+" "+row[4]+" "+row[5]] = row[9]
	}
	for key, want := range map[string]string{
		"kubernetes ReplicaSet/front-7c9 rollout": ActorKubernetes,
		"write Deployment/api api-patch":          ActorAgent,
		"write Pod/api-0 mitigation":              ActorAgent,
		"write Pod/db-0 mitigation":               ActorSDO,
	} {
		if got, ok := actors[key]; !ok || got != want {
			t.Errorf("row %q actor = %q (present %v), want %q\n%s", key, got, ok, want, response.Text)
		}
	}
	if !strings.Contains(response.Text, ",api,Deployment/api,api-patch,") {
		t.Errorf("the agent's write is placed on its component:\n%s", response.Text)
	}
}
