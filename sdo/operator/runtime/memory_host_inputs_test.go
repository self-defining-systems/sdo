package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type hostWidgets struct{}

func (hostWidgets) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if ref.Kind != "Widget" || ref.Name != "w1" {
		return nil, fmt.Errorf("not found: %s", ref.Name)
	}
	o := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"size": int64(3)}}}
	o.SetAPIVersion(ref.APIVersion)
	o.SetKind(ref.Kind)
	o.SetNamespace(ref.Namespace)
	o.SetName(ref.Name)
	return o, nil
}
func (h hostWidgets) CaptureObjects([]sdk.ObjectRef) ([]unstructured.Unstructured, error) {
	o, _ := h.Object(sdk.ObjectRef{APIVersion: "example.dev/v1", Kind: "Widget", Namespace: "toy", Name: "w1"})
	return []unstructured.Unstructured{*o}, nil
}

// extraInputProgram matches only when every input beyond the typed mirror
// reached it: scoped observations, pod admission and generic objects.
type extraInputProgram struct{}

func (extraInputProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "extra"} }
func (extraInputProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	deployments, err := v.ScopedDeployments("other")
	if err != nil {
		return program.Verdict{}, err
	}
	admission, ok := v.PodAdmission()
	widget, err := v.Object(sdk.ObjectRef{APIVersion: "example.dev/v1", Kind: "Widget", Namespace: "toy", Name: "w1"})
	if err != nil {
		return program.Verdict{}, err
	}
	captured, err := v.CaptureObjects(nil)
	if err != nil {
		return program.Verdict{}, err
	}
	size, _, _ := unstructured.NestedInt64(widget.Object, "spec", "size")
	if len(deployments) == 1 && ok && admission.Rejection == "quota exceeded" && size == 3 && len(captured) == 1 {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

// A program running in the host gets scoped observations, pod admission and
// generic objects from the controller's frozen view and the host's reader.
func TestRemoteProgramReceivesScopedAdmissionAndGenericObjects(t *testing.T) {
	ctx := context.Background()
	registry, err := program.NewRegistry([]program.Diagnoser{extraInputProgram{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	host, err := memoryhost.NewServer("c", "d", nil)
	if err != nil {
		t.Fatal(err)
	}
	host.WithPrograms(registry).WithObjects(hostWidgets{}, "")
	ready, _ := host.Ready(ctx, nil)
	rpc, _ := memoryTestRPC(t, host)
	remote, err := NewRemoteMemory(ready, mirroredTestHost(t, ctx, rpc, "c", "d", "toy"))
	if err != nil {
		t.Fatal(err)
	}
	target := sdk.ObservationTarget{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "other"}
	snapshot := sdktest.Snapshot{
		NamespaceName: "toy",
		Admission:     &sdk.PodAdmissionResult{Namespace: "toy", Rejection: "quota exceeded"},
	}
	snapshot.ScopedSnapshot = sdk.ScopedSnapshot{
		Version: 1, Generation: "g", Scope: sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}},
		Statuses:    []sdk.ObservationStatus{{Target: target, State: sdk.ObservationObserved}},
		Deployments: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "remote", Namespace: "other"}}},
	}
	view := &sdktest.RecordingReadView{ReadView: &sdktest.ReplayReadView{Snapshot: snapshot}}
	p, _ := remote.Programs.Diagnoser("extra")
	if verdict := program.RunDiagnose(ctx, p, view, program.Bindings{}, 5*time.Second); verdict.Result != program.Match {
		t.Fatalf("host program missed an input: %+v", verdict)
	}
}
