package replay

import (
	"context"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

func TestGenericClusterCapturesAndPatchesUnregisteredKinds(t *testing.T) {
	for _, ref := range []sdk.ObjectRef{{APIVersion: "example.test/v9", Kind: "NeverRegistered", Namespace: "other", Name: "x"}, {APIVersion: "example.test/v9", Kind: "ClusterCustom", Name: "policy"}, {APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: "app", Name: "data"}} {
		t.Run(ref.Kind, func(t *testing.T) {
			object := unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace}, "spec": map[string]any{"unknownField": "broken"}}}
			c, err := newCluster(sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{object}})
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Patch(context.Background(), ref, types.MergePatchType, []byte(`{"spec":{"unknownField":"fixed"}}`)); err != nil {
				t.Fatal(err)
			}
			snapshot, err := c.snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			got, err := snapshot.Object(ref)
			if err != nil {
				t.Fatal(err)
			}
			value, _, _ := unstructured.NestedString(got.Object, "spec", "unknownField")
			if value != "fixed" {
				t.Fatalf("generic field lost: %+v", got.Object)
			}
		})
	}
}

type genericReplayProgram struct{ ref sdk.ObjectRef }

func (p genericReplayProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "generic-object", Params: []program.ParamSpec{{Name: "target", Kind: program.Kind(p.ref.Kind), Required: true}, {Name: "desired", ValueKind: program.StringValue, Required: true}}}
}
func (p genericReplayProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	object, err := v.Object(p.ref)
	if err != nil {
		return program.Verdict{}, err
	}
	value, _, _ := unstructured.NestedString(object.Object, "spec", "unknownField")
	result := program.Match
	if value == "fixed" {
		result = program.NoMatch
	}
	return program.Verdict{Result: result}, nil
}
func (p genericReplayProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: p.ref, Path: "/spec/unknownField", Value: program.FromParam("desired")}}}, nil
}
func TestGenericObjectProductionExecutorReplay(t *testing.T) {
	for _, namespace := range []string{"app", "other", ""} {
		t.Run("namespace="+namespace, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "example.test/v9", Kind: "NeverRegistered", Namespace: namespace, Name: "x"}
			object := unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace}, "spec": map[string]any{"unknownField": "broken"}}}
			fixed := object.DeepCopy()
			_ = unstructured.SetNestedField(fixed.Object, "fixed", "spec", "unknownField")
			open := sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{object}}
			healthy := sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{*fixed}}
			p := genericReplayProgram{ref: ref}
			bindings := program.Bindings{Objects: program.ObjectBindings{"target": ref}, Values: map[string]any{"desired": "fixed"}}
			result := Check(context.Background(), p, p, nil, Case{ID: "generic", Accepted: []*unstructured.Unstructured{fixed.DeepCopy()}, Corpus: sdktest.CorpusCase{Open: open, Fixed: healthy, Healthy: healthy, Bindings: bindings, PeerBindings: bindings, RootCauseProgramIDs: []string{p.Spec().ID}}})
			if result.Status != "replayed" {
				t.Fatalf("production executor failed generic replay: %+v", result)
			}
		})
	}
}
