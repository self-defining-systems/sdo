package program_test

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// A synthetic map: a derived cache rebuilt from an authoritative store, and a
// stateless frontend.
var syntheticMap = program.AppMap{Components: []program.AppComponent{
	{Name: "front", Workload: "Deployment/front"},
	{Name: "copy", Workload: "Deployment/copy", Data: &program.DataRole{Engine: "redis", Role: program.DataRoleDerived, DerivedFrom: []string{"truth"}}},
	{Name: "truth", Workload: "StatefulSet/truth", HoldsData: true, Data: &program.DataRole{Engine: "mongodb", Role: program.DataRoleAuthoritative}},
}}

func ownedPod(name, ownerKind, owner string, ready bool, containers ...corev1.Container) corev1.Pod {
	yes := true
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: ownerKind, Name: owner, Controller: &yes}}},
		Spec:       corev1.PodSpec{Containers: containers},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}},
	}
}

func mapSnapshot() sdktest.Snapshot {
	yes := true
	m := syntheticMap
	return sdktest.Snapshot{
		NamespaceName: "app", ApplicationMap: &m,
		ReplicaSetList: []appsv1.ReplicaSet{{ObjectMeta: metav1.ObjectMeta{Name: "copy-1", Namespace: "app", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "copy", Controller: &yes}}}}},
		PodList: []corev1.Pod{
			ownedPod("copy-1-b", "ReplicaSet", "copy-1", false, corev1.Container{Name: "exporter", Image: "registry.example/exporter:1"}, corev1.Container{Name: "kv", Image: "redis:7.4"}),
			ownedPod("copy-1-a", "ReplicaSet", "copy-1", true, corev1.Container{Name: "exporter", Image: "registry.example/exporter:1"}, corev1.Container{Name: "kv", Image: "redis:7.4"}),
			ownedPod("truth-0", "StatefulSet", "truth", true, corev1.Container{Name: "db", Image: "mongo:7"}),
			ownedPod("front-x", "ReplicaSet", "front-1", true, corev1.Container{Name: "web", Image: "registry.example/front:1"}),
		},
	}
}

func podRef(name string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: name}
}

func TestProgramsResolveStoresFromTheApplicationMap(t *testing.T) {
	view := mapSnapshot().RuntimeReadView()
	m, err := view.AppMap()
	if err != nil {
		t.Fatal(err)
	}
	truth, ok := m.ComponentOf(view, podRef("truth-0"))
	if !ok || truth.Name != "truth" || !truth.HoldsData {
		t.Fatalf("ComponentOf(truth-0) = %+v, %v", truth, ok)
	}
	copies := m.Derived(truth.Name)
	if len(copies) != 1 || copies[0].Name != "copy" {
		t.Fatalf("Derived(truth) = %+v", copies)
	}
	if sources := m.Sources("copy"); len(sources) != 1 || sources[0].Name != "truth" {
		t.Fatalf("Sources(copy) = %+v", sources)
	}
	if c, ok := m.ComponentOf(view, podRef("copy-1-a")); !ok || c.Name != "copy" {
		t.Fatalf("ComponentOf follows Pod -> ReplicaSet -> Deployment: %+v %v", c, ok)
	}
	target, err := program.StoreTargetFor(view, copies[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	want := program.StoreTarget{Component: "copy", Engine: "redis", Pod: podRef("copy-1-a"), Container: "kv"}
	if target != want {
		t.Fatalf("StoreTargetFor(copy) = %+v, want the ready Pod's engine container %+v", target, want)
	}
	if err := target.Validate(); err != nil {
		t.Fatalf("resolved target does not validate: %v", err)
	}
	if target, err := program.StoreTargetFor(view, "truth"); err != nil || target.Container != "db" || target.Engine != "mongodb" {
		t.Fatalf("StoreTargetFor(truth) = %+v, %v", target, err)
	}
	for _, component := range []string{"front", "absent"} {
		if _, err := program.StoreTargetFor(view, component); err == nil {
			t.Errorf("StoreTargetFor(%s) resolved a store", component)
		}
	}
}

func TestAppMapReadIsRecordedAndReplayed(t *testing.T) {
	recorder := &sdktest.RecordingReadView{ReadView: mapSnapshot().RuntimeReadView()}
	if _, err := recorder.AppMap(); err != nil {
		t.Fatal(err)
	}
	replayed := sdktest.Snapshot{NamespaceName: "app", Readings: recorder.Calls}
	m, err := replayed.AppMap()
	if err != nil || len(m.Components) != 3 {
		t.Fatalf("replayed AppMap = %+v, %v", m, err)
	}
	if _, err := (sdktest.Snapshot{NamespaceName: "app"}).AppMap(); err == nil {
		t.Fatal("an unrecorded map read succeeded")
	}
}

type mapReader struct{}

func (mapReader) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "map-reader", Params: []program.ParamSpec{{Name: "store", Kind: "Pod", Required: true}}}
}

func (mapReader) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	m, err := v.AppMap()
	if err != nil {
		return program.Verdict{}, err
	}
	if _, ok := m.ComponentOf(v, b.Objects["store"]); !ok {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match}, nil
}

func TestDiagnoseWithoutAMapIsUnknown(t *testing.T) {
	b := program.Bindings{Objects: program.ObjectBindings{"store": podRef("truth-0")}}
	if got := program.RunDiagnose(context.Background(), mapReader{}, mapSnapshot().RuntimeReadView(), b, time.Second); got.Result != program.Match {
		t.Fatalf("with a map: %+v", got)
	}
	bare := mapSnapshot()
	bare.ApplicationMap = nil
	if got := program.RunDiagnose(context.Background(), mapReader{}, bare.RuntimeReadView(), b, time.Second); got.Result != program.Unknown {
		t.Fatalf("an unavailable map must make the verdict Unknown, got %+v", got)
	}
}

func TestVerdictCauseVocabulary(t *testing.T) {
	for name, test := range map[string]struct {
		verdict program.Verdict
		ok      bool
	}{
		"match out of reach":        {program.Verdict{Result: program.Match, CauseClass: "network-delay", Reach: program.ReachOutOfReach}, true},
		"match in scope":            {program.Verdict{Result: program.Match, CauseClass: "config-value-wrong", Reach: program.ReachInScope}, true},
		"class without reach":       {program.Verdict{Result: program.Match, CauseClass: "data-corrupt"}, true},
		"unknown class":             {program.Verdict{Result: program.Match, CauseClass: "gremlins", Reach: program.ReachInScope}, false},
		"unknown reach":             {program.Verdict{Result: program.Match, CauseClass: "network-loss", Reach: "elsewhere"}, false},
		"out of reach needs class":  {program.Verdict{Result: program.Match, Reach: program.ReachOutOfReach}, false},
		"reach on a no-match":       {program.Verdict{Result: program.NoMatch, CauseClass: "network-loss", Reach: program.ReachOutOfReach}, false},
		"plain match stays allowed": {program.Verdict{Result: program.Match}, true},
	} {
		if err := test.verdict.Validate(); (err == nil) != test.ok {
			t.Errorf("%s: Validate() = %v, want ok=%v", name, err, test.ok)
		}
	}
	if len(program.CauseClasses()) == 0 {
		t.Fatal("no cause classes")
	}
}

func TestEscalateIsTheOnlyActionOfItsPlan(t *testing.T) {
	link := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "truth"}
	escalate := program.Escalate{Reason: "the store's network path is slow outside the namespace", Resources: []sdk.ObjectRef{link}}
	if err := (program.Plan{Actions: []program.Action{escalate}}).Validate(); err != nil {
		t.Fatalf("sole Escalate: %v", err)
	}
	if err := (program.Plan{Actions: []program.Action{&escalate}}).Validate(); err != nil {
		t.Fatalf("sole *Escalate: %v", err)
	}
	mixed := program.Plan{Actions: []program.Action{escalate, program.DeletePod{Ref: podRef("truth-0")}}}
	if err := mixed.Validate(); err == nil || !strings.Contains(err.Error(), "only action") {
		t.Fatalf("Escalate with a repair validated: %v", err)
	}
	if err := (program.Plan{Actions: []program.Action{program.Escalate{}}}).Validate(); err == nil {
		t.Fatal("Escalate without a reason validated")
	}
}

func TestProposeSourceChangeValidates(t *testing.T) {
	deployment := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "front"}
	reason := "front saturates at the accepted replica count under the observed load; keep the scaled count"
	ok := program.ProposeSourceChange{Ref: deployment, Path: "spec.replicas", Value: program.FromObserved(deployment, "spec.replicas"), Reason: reason}
	if err := (program.Plan{Actions: []program.Action{program.ScaleWorkload{Ref: deployment, Replicas: program.FromParam("replicas")}, ok}}).Validate(); err != nil {
		t.Fatalf("valid proposal: %v", err)
	}
	for name, change := range map[string]program.ProposeSourceChange{
		"short reason":   {Ref: deployment, Path: "spec.replicas", Value: program.FromObserved(deployment, "spec.replicas"), Reason: "scale"},
		"source value":   {Ref: deployment, Path: "spec.replicas", Value: program.FromSource(deployment, "spec.replicas"), Reason: reason},
		"pointer path":   {Ref: deployment, Path: "/spec/replicas", Value: program.FromObserved(deployment, "spec.replicas"), Reason: reason},
		"unlisted path":  {Ref: deployment, Path: "metadata.labels", Value: program.FromObserved(deployment, "metadata.labels"), Reason: reason},
		"no namespace":   {Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "front"}, Path: "spec.replicas", Value: program.FromParam("replicas"), Reason: reason},
		"secret":         {Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "s"}, Path: "data[k]", Value: program.FromParam("v"), Reason: reason},
		"missing values": {Ref: deployment, Path: "spec.replicas", Reason: reason},
	} {
		if err := change.Validate(); err == nil {
			t.Errorf("%s: validated", name)
		}
	}
}

func TestSetFieldAcceptsAServicePortList(t *testing.T) {
	service := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "front"}
	if err := program.ValidateSetField(program.SetField{Ref: service, Path: "spec.ports", Value: program.FromSource(service, "spec.ports"), Reason: "restore the accepted ports"}); err != nil {
		t.Fatalf("spec.ports: %v", err)
	}
}
