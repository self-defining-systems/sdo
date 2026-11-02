package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kubefake "k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
	"testing/quick"
)

func TestDatabaseReferencesNeverResolveHostCredentials(t *testing.T) {
	a := databaseAction()
	const canary = "fake-host-secret-canary"
	t.Setenv("APP_PASSWORD", canary)
	if err := quick.Check(func(suffix uint16) bool {
		copy := a
		copy.Password.Env = fmt.Sprintf("PASSWORD_%d", suffix)
		t.Setenv(copy.Password.Env, canary)
		raw, err := json.Marshal(copy)
		script := mongoScript(copy.Target, &copy)
		return err == nil && !strings.Contains(string(raw)+script, canary) && strings.Contains(script, copy.Password.Env)
	}, &quick.Config{MaxCount: 100}); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseMalformedContractDoesNotPanic(t *testing.T) {
	a := planActionState(&contracts.Action{Kind: &contracts.Action_EnsureDatabaseUser{EnsureDatabaseUser: &contracts.EnsureDatabaseUser{}}})
	if err := (program.Plan{Actions: []program.Action{a}}).Validate(); err == nil {
		t.Fatal("accepted absent target and password")
	}
}

func TestDatabaseCatalogBatchProjectsEarlierUsers(t *testing.T) {
	a := databaseAction()
	ref := a.Target.Pod
	e, c, oldRef := runtimeActionFixture()
	pod := c.objects[oldRef].DeepCopy()
	pod.SetName(ref.Name)
	pod.SetNamespace(ref.Namespace)
	_ = unstructured.SetNestedSlice(pod.Object, []any{map[string]any{"name": "mongo"}}, "spec", "containers")
	db := &databaseFake{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: pod}}}
	e.Client = db
	b := a
	b.User = "second"
	r, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{a, b}}, false)
	if err != nil || !r.VerificationPassed || len(db.users) != 2 {
		t.Fatalf("%+v %v", r, err)
	}
}

func TestNewActionsAppearInDescriptorMenuAndRoundTrip(t *testing.T) {
	entries := rawMenuActions()
	for _, name := range []string{"RemoveServicePort", "EnsureDatabaseUser"} {
		if _, ok := entries[name]; !ok {
			t.Fatalf("missing menu action %s", name)
		}
	}
	p, err := menuRawPlan(entries["RemoveServicePort"].action, map[string]string{"ref": "Service/app/discovery", "name": "unused", "reason": "synthetic"}, &program.ValueInputs{})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Validate(); err != nil {
		t.Fatal(err)
	}
	p, err = menuRawPlan(entries["EnsureDatabaseUser"].action, map[string]string{"target": `{"engine":"mongodb","pod":{"api_version":"v1","kind":"Pod","namespace":"app","name":"database"},"container":"mongo","database":"app"}`, "user": "app", "roles": `[{"database":"app","name":"readWrite"}]`, "password": `{"env":"APP_PASSWORD"}`}, &program.ValueInputs{})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Validate(); err != nil {
		t.Fatal(err)
	}
	encoded, err := (PlanApplicationResult{Plan: p}).Proto()
	if err != nil {
		t.Fatal(err)
	}
	decoded := planApplicationState(encoded).Result.Plan
	if err = decoded.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseUnknownMenuReceiptEarnsNoRepairCredit(t *testing.T) {
	a := databaseAction()
	ref := a.Target.Pod
	object := planObject(ref, map[string]any{"containers": []any{map[string]any{"name": "mongo"}}})
	object.SetUID("one")
	object.SetResourceVersion("1")
	client := &databaseFake{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}, ignore: true}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "database-uncertain", Namespace: "app", RepairPolicy: "recorded-actions"}, nil, nil, nil, PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}, 5)
	loop.MenuConfig = productionResponderMenu()
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "EnsureDatabaseUser", RoundLimit: 1}, Parameters: map[string]string{"target": `{"engine":"mongodb","pod":{"api_version":"v1","kind":"Pod","namespace":"app","name":"database"},"container":"mongo","database":"app"}`, "user": "app", "roles": `[{"database":"app","name":"readWrite"}]`, "password": `{"env":"APP_PASSWORD"}`}})
	if response.Report == nil || len(loop.MenuRepairActions()) != 0 {
		t.Fatalf("unconfirmed catalog change earned repair credit: %+v", response)
	}
	for _, round := range response.Report.Rounds {
		for _, receipt := range round.Actions {
			if receipt.State == contracts.MitigationActionReceipt_STATE_APPLIED {
				t.Fatal("unknown effect reported applied")
			}
		}
	}
}

func TestDatabaseLiveReadRejectsReplacedSnapshotPod(t *testing.T) {
	a := databaseAction()
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: a.Target.Pod.Name, Namespace: "app", UID: "snapshot"}}
	replacement := pod.DeepCopy()
	replacement.UID = "replacement"
	client := kubefake.NewSimpleClientset(replacement)
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{pod}}), Client: client}
	if _, err := view.DatabaseUsers(a.Target); err == nil || !strings.Contains(err.Error(), "identity differs") {
		t.Fatalf("stale Pod snapshot accepted: %v", err)
	}
	for _, action := range client.Actions() {
		if action.GetResource().Resource != "pods" || action.GetVerb() != "get" {
			t.Fatalf("unexpected sensitive operation: %v", action)
		}
	}
}

