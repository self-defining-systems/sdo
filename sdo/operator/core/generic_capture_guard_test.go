package core

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	discoveryfake "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/scheme"
	clienttesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type capturedFieldRepair struct{ ref sdk.ObjectRef }

func (p capturedFieldRepair) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "captured-field", Params: []program.ParamSpec{{Name: "desired", ValueKind: program.StringValue, Required: true}}}
}
func (p capturedFieldRepair) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	o, err := v.Object(p.ref)
	if err != nil {
		return program.Verdict{}, err
	}
	mode, _, _ := unstructured.NestedString(o.Object, "spec", "captureTest")
	result := program.Match
	if mode == "fixed" {
		result = program.NoMatch
	}
	return program.Verdict{Result: result}, nil
}
func (p capturedFieldRepair) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: p.ref, Path: "/spec/captureTest", Value: program.FromParam("desired")}}}, nil
}

func TestMutableKindCaptureReplayGuard(t *testing.T) {
	// Derive the existing CLI kind universe and typed SDK mutation universe from
	// their owning source. A newly added mutation kind automatically joins this
	// contract, rather than requiring another hand-maintained capture inventory.
	kinds := map[string]bool{"PreviouslyUnknownCustomResource": true}
	data, err := os.ReadFile(filepath.Join("..", "sdk", "object_ref.go"))
	if err != nil {
		t.Fatal(err)
	}
	tree, err := parser.ParseFile(token.NewFileSet(), "object_ref.go", data, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(tree, func(node ast.Node) bool {
		spec, ok := node.(*ast.ValueSpec)
		if !ok || len(spec.Names) != 1 || spec.Names[0].Name != "objectKinds" {
			return true
		}
		literal := spec.Values[0].(*ast.CompositeLit)
		for _, entry := range literal.Elts {
			value := entry.(*ast.KeyValueExpr).Value.(*ast.BasicLit)
			kind, _ := strconv.Unquote(value.Value)
			kinds[kind] = true
		}
		return false
	})
	data, err = os.ReadFile(filepath.Join("..", "sdk", "program", "fields.go"))
	if err != nil {
		t.Fatal(err)
	}
	tree, err = parser.ParseFile(token.NewFileSet(), "fields.go", data, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(tree, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if ok && literal.Kind == token.STRING {
			value, _ := strconv.Unquote(literal.Value)
			parts := strings.Split(value, "/")
			if len(parts) >= 2 && parts[len(parts)-1] != "" && token.IsIdentifier(parts[len(parts)-1]) && parts[len(parts)-1][0] >= 'A' && parts[len(parts)-1][0] <= 'Z' {
				kinds[parts[len(parts)-1]] = true
			}
		}
		return true
	})
	for kind := range kinds {
		if kind == "Secret" {
			continue
		}
		t.Run(kind, func(t *testing.T) {
			gvk := schema.GroupVersionKind{Group: "synthetic.example", Version: "v1", Kind: kind}
			for known := range scheme.Scheme.AllKnownTypes() {
				if known.Kind == kind && known.Version == "v1" {
					gvk = known
					break
				}
			}
			gvr, _ := meta.UnsafeGuessKindToResource(gvk)
			ref := sdk.ObjectRef{APIVersion: gvk.GroupVersion().String(), Kind: kind, Namespace: "app", Name: "object"}
			object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": kind, "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace}, "spec": map[string]any{"captureTest": "broken"}}}
			fake := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(kruntime.NewScheme(), map[schema.GroupVersionResource]string{gvr: kind + "List"}, object)
			discover := &discoveryfake.FakeDiscovery{Fake: &clienttesting.Fake{Resources: []*metav1.APIResourceList{{GroupVersion: ref.APIVersion, APIResources: []metav1.APIResource{{Name: gvr.Resource, Kind: kind, Namespaced: true, Verbs: []string{"get", "list", "patch"}}}}}}}
			snapshot, err := sdktest.CaptureSnapshot(DetectionSnapshot{NamespaceName: "app", GenericCapture: &GenericCapture{Namespace: "app", Client: fake, Discovery: discover}}, "app")
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(snapshot)
			var replay sdktest.Snapshot
			if err = json.Unmarshal(raw, &replay); err != nil {
				t.Fatal(err)
			}
			p := capturedFieldRepair{ref: ref}
			result, err := sdktest.CheckMitigation(p, p, sdktest.CorpusCase{Open: replay, Bindings: program.Bindings{Values: map[string]any{"desired": "fixed"}}})
			if err != nil || result.Status != sdktest.MitigationReplayPassed {
				t.Fatalf("mutable kind lost capture/replay coverage: %+v %v", result, err)
			}
		})
	}
}
