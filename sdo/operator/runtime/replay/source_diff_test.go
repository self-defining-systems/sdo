package replay

import (
	"context"
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/program/testdata/driftbound"
	"sdo.dev/operator/sdk/sdktest"
)

func TestSourceAbsentDeletionReplaysThroughProductionExecutor(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding", Name: "toy-binding"}
	open := sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name}}}}}
	fixed := sdktest.Snapshot{NamespaceName: "app"}
	args, _ := json.Marshal([]any{ref})
	c := Case{ID: "source-absent-binding", Corpus: sdktest.CorpusCase{Open: open, Fixed: fixed, Healthy: fixed, Bindings: program.Bindings{Objects: program.ObjectBindings{"binding": ref}}, RootCauseProgramIDs: []string{"source-absent-binding"}, Recordings: []sdktest.PrimitiveCall{{Phase: "open", Method: "DiffAgainstSource", Arguments: args, Result: json.RawMessage(`{"changed":true,"source_absent":true}`)}, {Phase: "fixed", Method: "DiffAgainstSource", Arguments: args, Result: json.RawMessage(`{"changed":false,"source_absent":true}`)}}}}
	p := driftbound.Program{}
	result := CheckRepair(context.Background(), p, p, c)
	if result.Status != "replayed" || result.Post.Result != program.NoMatch {
		t.Fatalf("measured delete did not clear source diff: %+v", result)
	}
}

func TestPostPlanDiffUsesAPIStateAndNeverFixedReplies(t *testing.T) {
	ctx := context.Background()
	ref := sdk.ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding", Name: "toy-binding"}
	initial := sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name}}}}}
	cluster, err := newCluster(initial)
	if err != nil {
		t.Fatal(err)
	}
	source, err := newSource(Case{})
	if err != nil {
		t.Fatal(err)
	}
	args, _ := json.Marshal([]any{ref})
	view := newReadView(ctx, sdktest.Snapshot{NamespaceName: "app"}, []sdktest.PrimitiveCall{{Method: "DiffAgainstSource", Arguments: args, Result: json.RawMessage(`{"changed":false,"source_absent":true}`)}}, source)
	view.external = cluster
	if diff, err := view.DiffAgainstSource(ref); err != nil || !diff.Changed || !diff.SourceAbsent {
		t.Fatalf("fixed reply hid undeleted object: %+v %v", diff, err)
	}
	current, err := cluster.Get(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Delete(ctx, ref, current.GetUID(), current.GetResourceVersion()); err != nil {
		t.Fatal(err)
	}
	if diff, err := view.DiffAgainstSource(ref); err != nil || diff.Changed || !diff.SourceAbsent {
		t.Fatalf("API 404 was not confirmed absence: %+v %v", diff, err)
	}
	if findings, err := sdk.DetectObserved(ctx, driftbound.Detector{}, view); err != nil || len(findings) != 0 {
		t.Fatalf("post-plan detector did not use measured diff: %+v %v", findings, err)
	}
	missing := newReadView(ctx, sdktest.Snapshot{NamespaceName: "app"}, nil, source)
	if _, err := missing.DiffAgainstSource(ref); err == nil {
		t.Fatal("uncaptured object established absence")
	}
}
