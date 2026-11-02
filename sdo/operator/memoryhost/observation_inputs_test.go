package memoryhost

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// fakeObjects is the host's generic reader: it holds one custom resource and
// records the names asked for, so tests can see which reads reached it.
type fakeObjects struct{ asked []string }

func (f *fakeObjects) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	f.asked = append(f.asked, ref.Kind+"/"+ref.Name)
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
func (f *fakeObjects) CaptureObjects([]sdk.ObjectRef) ([]unstructured.Unstructured, error) {
	o, _ := f.Object(sdk.ObjectRef{APIVersion: "example.dev/v1", Kind: "Widget", Namespace: "toy", Name: "w1"})
	return []unstructured.Unstructured{*o}, nil
}

// inputReader reads every input the controller's view provides beyond the
// typed mirror, and reports what it saw as verdict evidence.
type inputReader struct{}

func (inputReader) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "inputs"} }
func (inputReader) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	var facts []string
	add := func(name string, value any, err error) {
		if err != nil {
			value = "error: " + err.Error()
		}
		facts = append(facts, fmt.Sprintf("%s=%v", name, value))
	}
	deployments, err := v.ScopedDeployments("other")
	add("scoped", len(deployments), err)
	if len(deployments) > 0 {
		add("scoped-name", deployments[0].Name, nil)
	}
	object, err := v.Object(sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "other", Name: "remote"})
	if err == nil {
		add("scoped-object", object.GetName(), nil)
	} else {
		add("scoped-object", nil, err)
	}
	admission, ok := v.PodAdmission()
	add("admission", fmt.Sprintf("%t/%s", ok, admission.Rejection), nil)
	generic, err := v.Object(sdk.ObjectRef{APIVersion: "example.dev/v1", Kind: "Widget", Namespace: "toy", Name: "w1"})
	if err == nil {
		size, _, _ := unstructured.NestedInt64(generic.Object, "spec", "size")
		add("generic", size, nil)
	} else {
		add("generic", nil, err)
	}
	captured, err := v.CaptureObjects(nil)
	add("captured", len(captured), err)
	return program.Verdict{Result: program.NeedsJudgement, Question: strings.Join(facts, ";")}, nil
}

func scopedObservation(t *testing.T) *pb.MemoryObservation {
	t.Helper()
	target := sdk.ObservationTarget{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "other"}
	scoped := sdk.ScopedSnapshot{
		Version: 1, Generation: "g1", Scope: sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}},
		Statuses:    []sdk.ObservationStatus{{Target: target, State: sdk.ObservationObserved}},
		Deployments: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "remote", Namespace: "other"}}},
	}
	return &pb.MemoryObservation{
		Scoped:       wire(t, scoped),
		PodAdmission: wire(t, sdk.PodAdmissionResult{Namespace: "toy", Rejection: "quota exceeded"}),
	}
}

func wire(t *testing.T, value any) *structpb.Struct {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	out := &structpb.Struct{}
	if err = out.UnmarshalJSON(raw); err != nil {
		t.Fatal(err)
	}
	return out
}

func readyServer(t *testing.T, detectors ...sdk.Detector) (*Server, *fakeObjects) {
	t.Helper()
	registry, err := program.NewRegistry([]program.Diagnoser{inputReader{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewServer("c", "d", detectors)
	if err != nil {
		t.Fatal(err)
	}
	objects := &fakeObjects{}
	s.WithPrograms(registry).WithObjects(objects, "sdo-control")
	if err = s.Apply(&pb.MirrorRequest{Sequence: 1, ResetMirror: true, Namespace: "toy", Complete: true}); err != nil {
		t.Fatal(err)
	}
	return s, objects
}

// A program in the host receives scoped observations, pod admission, generic
// objects and capture, exactly as the controller's in-process view gives them.
func TestProgramReceivesScopedAdmissionAndGenericObjects(t *testing.T) {
	s, objects := readyServer(t)
	got, err := s.Diagnose(context.Background(), &pb.DiagnoseRequest{Input: &pb.MemoryProgramInput{ProgramId: "inputs", MirrorSequence: 1, Observation: scopedObservation(t)}})
	if err != nil || got.Error != "" {
		t.Fatal(got, err)
	}
	facts := got.Verdict.Question
	for _, want := range []string{
		"scoped=1", "scoped-name=remote", "scoped-object=remote",
		"admission=true/quota exceeded", "generic=3", "captured=1",
	} {
		if !strings.Contains(facts, want) {
			t.Errorf("program did not see %q in %q", want, facts)
		}
	}
	for _, asked := range objects.asked {
		if strings.Contains(asked, "remote") {
			t.Errorf("scoped target read fell through to the generic reader: %v", objects.asked)
		}
	}
}

// Without the scoped field the program's first scoped read fails closed; the
// absent input is unavailable, never an empty list.
func TestProgramWithoutScopedObservationFailsClosed(t *testing.T) {
	s, _ := readyServer(t)
	got, err := s.Diagnose(context.Background(), &pb.DiagnoseRequest{Input: &pb.MemoryProgramInput{ProgramId: "inputs", MirrorSequence: 1}})
	if err != nil || got.Error != "" {
		t.Fatal(got, err)
	}
	if got.Verdict.Result != pb.Verdict_RESULT_UNKNOWN || !strings.Contains(got.Verdict.Evidence[0].Observed, "scoped observation") {
		t.Fatalf("absent scope read as present: %v", got.Verdict)
	}
}

// Generic reads never reach the SDO control namespace.
func TestGenericReadRefusesControlNamespace(t *testing.T) {
	s, objects := readyServer(t)
	reads := s.objectReads(sdktest.Snapshot{NamespaceName: "toy"}, sdktest.Snapshot{}, sdktest.Snapshot{}, "toy")
	if _, err := reads.Object(sdk.ObjectRef{APIVersion: "example.dev/v1", Kind: "Widget", Namespace: "sdo-control", Name: "w1"}); err == nil {
		t.Fatal("control namespace read allowed")
	}
	if len(objects.asked) != 0 {
		t.Fatalf("control namespace read reached the cluster reader: %v", objects.asked)
	}
}

type observedInputDetector struct{}

func (observedInputDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: "input-detector"} }
func (observedInputDetector) Detect(_ context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	scoped := sdk.ScopedSource(v)
	deployments, err := scoped.ScopedDeployments("other")
	if err != nil {
		return nil, err
	}
	admission, _ := v.PodAdmission()
	reader, ok := v.(sdk.ObjectReader)
	if !ok {
		return nil, fmt.Errorf("detector view is not an object reader")
	}
	object, err := reader.Object(sdk.ObjectRef{APIVersion: "example.dev/v1", Kind: "Widget", Namespace: "toy", Name: "w1"})
	if err != nil {
		return nil, err
	}
	return []sdk.Finding{{Summary: fmt.Sprintf("%d/%s/%s", len(deployments), admission.Rejection, object.GetName())}}, nil
}

func TestDetectorReceivesScopedAdmissionAndGenericObjects(t *testing.T) {
	s, _ := readyServer(t, observedInputDetector{})
	got, err := s.Evaluate(context.Background(), &pb.EvaluateRequest{DetectorIds: []string{"input-detector"}, MirrorSequence: 1, Observation: scopedObservation(t)})
	if err != nil || got.Results[0].Error != "" {
		t.Fatal(got, err)
	}
	if summary := got.Results[0].Findings[0].Summary; summary != "1/quota exceeded/w1" {
		t.Fatalf("detector saw %q", summary)
	}
}

func TestInvalidScopedObservationIsRejected(t *testing.T) {
	s, _ := readyServer(t)
	bad := &pb.MemoryObservation{Scoped: wire(t, sdk.ScopedSnapshot{Version: 7})}
	if _, err := s.Diagnose(context.Background(), &pb.DiagnoseRequest{Input: &pb.MemoryProgramInput{ProgramId: "inputs", MirrorSequence: 1, Observation: bad}}); err == nil {
		t.Fatal("invalid scoped observation accepted")
	}
}
