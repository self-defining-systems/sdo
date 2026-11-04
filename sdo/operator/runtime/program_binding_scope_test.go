package runtime

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// A detector's named binding is its claim about which object its own
// playbooks repair. It must not become the target of every other program that
// happens to use the same role name: two per-journey detectors that each name
// their handler would otherwise bind each other's restart programs to the
// wrong component (live: one stall playbook ran another journey's exec repair
// against the wrong Redis).
func TestNamedFindingBindingsReachOnlyTheFindingsPlaybooks(t *testing.T) {
	ref := func(name string) sdk.ObjectRef {
		return sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: name}
	}
	snapshot := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "gateway", Namespace: "app"}}}}
	for _, name := range []string{"reader-a", "reader-b"} {
		snapshot.DeploymentList = append(snapshot.DeploymentList, appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}})
	}
	var diagnosers []program.Diagnoser
	for _, id := range []string{"restart-a", "restart-b", "generic-restart"} {
		diagnosers = append(diagnosers, candidateProgram{spec: program.ProgramSpec{ID: id, Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict {
			return program.Verdict{Result: program.Match}
		}})
	}
	registry, err := program.NewRegistry(diagnosers, nil)
	if err != nil {
		t.Fatal(err)
	}
	gateway := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "gateway"}
	journey := func(detector, playbook, target string) sdk.Finding {
		return sdk.Finding{DetectorID: detector, RuleID: detector, Status: sdk.FindingActive, PrimaryResource: gateway, Fingerprint: detector,
			Playbooks: []string{playbook}, ParameterBindings: map[string]sdk.ObjectRef{"deployment": ref(target)}}
	}
	findings := []sdk.Finding{journey("journey-a", "restart-a", "reader-a"), journey("journey-b", "restart-b", "reader-b")}
	runs := SelectProgramCandidates(context.Background(), registry, &sdktest.ReplayReadView{Snapshot: snapshot}, findings, time.Second)
	got := map[string]string{}
	for _, run := range runs {
		got[run.ProgramID+"/"+run.Bindings.Objects["deployment"].Name] = run.FindingFingerprint
	}
	want := map[string]string{"restart-a/reader-a": "journey-a", "restart-b/reader-b": "journey-b"}
	if len(got) != len(want) || got["restart-a/reader-a"] != "journey-a" || got["restart-b/reader-b"] != "journey-b" {
		t.Fatalf("candidates = %v, want only each detector's own playbook on its own target %v", got, want)
	}

	// A finding that names no playbook (generated, drift and generic health
	// findings) still offers its named binding to every program.
	unlisted := journey("drift", "", "reader-a")
	unlisted.Playbooks = nil
	runs = SelectProgramCandidates(context.Background(), registry, &sdktest.ReplayReadView{Snapshot: snapshot}, []sdk.Finding{unlisted}, time.Second)
	if len(runs) != 3 {
		t.Fatalf("unlisted finding bound %d programs, want all 3: %+v", len(runs), runs)
	}
}
