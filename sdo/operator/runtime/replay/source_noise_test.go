package replay

import (
	"context"
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	operator "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type sourceNoiseProgram struct{}

func (sourceNoiseProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "source-noise", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}}
}
func (sourceNoiseProgram) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	diff, err := v.DiffAgainstSource(b.Objects["deployment"])
	result := program.NoMatch
	if diff.Changed {
		result = program.Match
	}
	return program.Verdict{Result: result}, err
}
func (sourceNoiseProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: b.Objects["deployment"]}}}, nil
}

// Synthetic captured pair: the repaired image matches source while the
// deployment controller's revision advances independently of that repair.
func TestCapturedSourceRepairIgnoresDeploymentRevisionBump(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	accepted := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace, "annotations": map[string]any{"deployment.kubernetes.io/revision": "1"}},
		"spec": map[string]any{"selector": map[string]any{"matchLabels": map[string]any{"app": "api"}}, "template": map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "api"}}, "spec": map[string]any{"containers": []any{map[string]any{"name": "api", "image": "api:good"}}}}},
	}}
	fixed := accepted.DeepCopy()
	if err := operator.RecordAcceptance(fixed, accepted); err != nil {
		t.Fatal(err)
	}
	annotations := fixed.GetAnnotations()
	annotations["deployment.kubernetes.io/revision"] = "3"
	fixed.SetAnnotations(annotations)
	faulted := fixed.DeepCopy()
	containers, _, _ := unstructured.NestedSlice(faulted.Object, "spec", "template", "spec", "containers")
	containers[0].(map[string]any)["image"] = "api:broken"
	_ = unstructured.SetNestedSlice(faulted.Object, containers, "spec", "template", "spec", "containers")
	capture := func(object *unstructured.Unstructured) sdktest.Snapshot {
		t.Helper()
		captured, err := sdktest.CaptureSnapshot(sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{*object}}, "app")
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(captured)
		if err != nil {
			t.Fatal(err)
		}
		var stored sdktest.Snapshot
		if err := json.Unmarshal(data, &stored); err != nil {
			t.Fatal(err)
		}
		return stored
	}
	p := sourceNoiseProgram{}
	c := Case{ID: "synthetic-revision-pair", Accepted: []*unstructured.Unstructured{accepted}, Corpus: sdktest.CorpusCase{Open: capture(faulted), Fixed: capture(fixed), Healthy: capture(fixed), Bindings: program.Bindings{Objects: program.ObjectBindings{"deployment": ref}}, RootCauseProgramIDs: []string{p.Spec().ID}}}
	s, err := newSource(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		snapshot sdktest.Snapshot
		want     program.Result
	}{{"faulted", c.Corpus.Open, program.Match}, {"fixed", c.Corpus.Fixed, program.NoMatch}} {
		t.Run(tc.name, func(t *testing.T) {
			verdict, err := p.Diagnose(context.Background(), newReadView(context.Background(), tc.snapshot, nil, s), c.Corpus.Bindings)
			if err != nil || verdict.Result != tc.want {
				t.Fatalf("diagnosis: %+v %v", verdict, err)
			}
		})
	}
	result := Check(context.Background(), p, p, nil, c)
	if result.Status != "replayed" || result.Post.Result != program.NoMatch {
		t.Fatalf("source repair replay: %+v", result)
	}
}
