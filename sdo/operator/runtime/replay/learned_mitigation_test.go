package replay

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Learned mitigations are replayed through the plan builder the incident stage
// uses. These programs are hand-written copies of the two shapes that passed
// validation and then failed on the repeat: a plan that names a list element
// through edits, and a plan derived from pod admission and a generic object.

type emptyPlanProgram struct{ fixtureProgram }

func (emptyPlanProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{}, nil
}

func TestReplayRejectsAMitigationThatReturnsAnEmptyPlan(t *testing.T) {
	p := emptyPlanProgram{fixtureProgram{mode: "delete"}}
	r := Check(context.Background(), p, p, nil, fixture("delete"))
	if r.Status == "replayed" || !strings.Contains(strings.Join(r.Errors, "; "), "empty plan") {
		t.Fatalf("an empty plan must fail validation, got %+v", r)
	}
}

type listEditProgram struct{}

func (listEditProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "list-edit"} }
func (listEditProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	for _, s := range v.Services() {
		for _, port := range s.Spec.Ports {
			if port.Name == "drop" {
				return program.Verdict{Result: program.Match}, nil
			}
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func (listEditProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	edit := program.ListEdit{Path: "/spec/ports", Match: map[string]any{"name": "drop"}, Op: "remove"}
	return program.Plan{Actions: []program.Action{program.PatchObject{Ref: ref, Edits: []program.ListEdit{edit}, Reason: "remove the injected port"}}}, nil
}

func TestReplayAcceptsAListEditMitigationTheStageCanApply(t *testing.T) {
	meta := metav1.ObjectMeta{Name: "api", Namespace: "app"}
	ports := func(names ...string) []corev1.ServicePort {
		var out []corev1.ServicePort
		for i, name := range names {
			out = append(out, corev1.ServicePort{Name: name, Port: int32(80 + i)})
		}
		return out
	}
	open := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: meta, Spec: corev1.ServiceSpec{Ports: ports("keep", "drop")}}}}
	healthy := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: meta, Spec: corev1.ServiceSpec{Ports: ports("keep")}}}}
	c := Case{ID: "list-edit", Corpus: sdktest.CorpusCase{Open: open, Healthy: healthy, Fixed: healthy, RootCauseProgramIDs: []string{"list-edit"}}}
	p := listEditProgram{}
	if r := Check(context.Background(), p, p, nil, c); r.Status != "replayed" {
		t.Fatalf("%+v", r)
	}
}

var quotaRejection = regexp.MustCompile(`failed quota: ([^: ]+):`)

type admissionQuotaProgram struct{}

func (admissionQuotaProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "admission-quota"}
}
func admissionQuota(v program.ReadView) (sdk.ObjectRef, bool) {
	admission, ok := v.PodAdmission()
	if !ok || admission.Admitted {
		return sdk.ObjectRef{}, false
	}
	m := quotaRejection.FindStringSubmatch(admission.Rejection)
	if len(m) != 2 {
		return sdk.ObjectRef{}, false
	}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ResourceQuota", Namespace: admission.Namespace, Name: m[1]}
	if _, err := v.Object(ref); err != nil {
		return sdk.ObjectRef{}, false
	}
	return ref, true
}
func (admissionQuotaProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	if _, ok := admissionQuota(v); ok {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func (admissionQuotaProgram) Mitigate(_ context.Context, v program.ReadView, _ program.Bindings) (program.Plan, error) {
	quota, ok := admissionQuota(v)
	if !ok {
		return program.Plan{}, nil
	}
	return program.Plan{Actions: []program.Action{program.DeleteObject{Ref: quota, Reason: "remove the quota named by pod admission"}}}, nil
}

func TestReplayRunsMitigationsOnTheAdmissionAndObjectsDiagnosisSaw(t *testing.T) {
	quota := unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ResourceQuota", "metadata": map[string]any{"name": "tight", "namespace": "app"}}}
	open := sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{quota}, Admission: &sdk.PodAdmissionResult{Namespace: "app", Rejection: "failed quota: tight: limits.memory"}}
	healthy := sdktest.Snapshot{NamespaceName: "app", Admission: &sdk.PodAdmissionResult{Namespace: "app", Admitted: true}}
	c := Case{ID: "admission-quota", Corpus: sdktest.CorpusCase{Open: open, Healthy: healthy, Fixed: healthy, RootCauseProgramIDs: []string{"admission-quota"}}}
	p := admissionQuotaProgram{}
	if r := Check(context.Background(), p, p, nil, c); r.Status != "replayed" {
		t.Fatalf("%+v", r)
	}
}

// Ratchet: validation has one way to build a learned mitigation's plan, the
// runtime builder the stage uses. A second direct call would let the two drift.
func TestReplayBuildsMitigationPlansOnlyThroughTheRuntimeBuilder(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".go") || strings.HasSuffix(f.Name(), "_test.go") {
			continue
		}
		body, err := os.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), ".Mitigate(") {
			t.Errorf("%s calls Mitigate directly; use runtime.BuildMitigationPlan", f.Name())
		}
	}
}

// A mitigation that fails the stage's plan builder is reported as that: the
// reflection worker reads this text to decide what to fix.
func TestReplayNamesTheStagePlanBuildInAFailure(t *testing.T) {
	p := emptyPlanProgram{fixtureProgram{mode: "delete"}}
	r := Check(context.Background(), p, p, nil, fixture("delete"))
	if !strings.Contains(strings.Join(r.Errors, "; "), "the plan the incident stage would build from this case's captured open view failed") {
		t.Fatalf("failure does not say which plan failed: %+v", r.Errors)
	}
}

func TestReplayMitigationFailureNamesAnUncapturedRead(t *testing.T) {
	err := mitigationBuildError(fmt.Errorf("%w: apps/v1 Deployment app/api", sdk.ErrObjectNotCaptured))
	if !strings.Contains(err.Error(), "capture it in the case") {
		t.Fatalf("no hint for an uncaptured read: %v", err)
	}
}

// Restoring an object the incident created and accepted source never declared
// is expressed as DeleteObject and replays. Reconciling it from source cannot
// work, live or offline, and the failure says which action to use.
type reconcileAbsentProgram struct{ fixtureProgram }

func (reconcileAbsentProgram) Mitigate(_ context.Context, _ program.ReadView, _ program.Bindings) (program.Plan, error) {
	ref := sdk.ObjectRef{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "api"}
	return program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, nil
}

func TestReplayRestoreToAbsentIsDeleteObjectAndReconcileSaysSo(t *testing.T) {
	deleting := fixtureProgram{mode: "delete"}
	if r := Check(context.Background(), deleting, deleting, nil, fixture("delete")); r.Status != "replayed" {
		t.Fatalf("restore-to-absent by DeleteObject must replay: %+v", r)
	}
	reconciling := reconcileAbsentProgram{fixtureProgram{mode: "delete"}}
	r := Check(context.Background(), reconciling, reconciling, nil, fixture("delete"))
	text := strings.Join(r.Errors, "; ")
	if r.Status == "replayed" || !strings.Contains(text, "DeleteObject") {
		t.Fatalf("reconciling a source-absent object must fail naming DeleteObject: %+v", r)
	}
}
