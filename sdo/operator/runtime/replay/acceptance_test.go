package replay

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	operator "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func TestReplayAcceptancePreservesCreatedIdentity(t *testing.T) {
	for _, kind := range []string{"Service", "ConfigMap", "Deployment"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			c, err := newCluster(sdktest.Snapshot{NamespaceName: "app"})
			if err != nil {
				t.Fatal(err)
			}
			version := "v1"
			if kind == "Deployment" {
				version = "apps/v1"
			}
			ref := sdk.ObjectRef{APIVersion: version, Kind: kind, Namespace: "app", Name: "created"}
			source := &unstructured.Unstructured{Object: map[string]any{"apiVersion": version, "kind": kind, "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace}}}
			created, err := c.Create(ctx, ref, source)
			if err != nil {
				t.Fatal(err)
			}
			baseline := created.DeepCopy()
			if err := operator.RecordAcceptance(baseline, source); err != nil {
				t.Fatal(err)
			}
			record := baseline.GetAnnotations()["sdo.dev/accepted-paths"]
			if record == "" {
				t.Fatal("production acceptance owner produced no record")
			}
			if err := c.RecordAcceptance(ctx, ref, record, created); err != nil {
				t.Fatal(err)
			}
			stored, err := c.Get(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			if stored.GetUID() != created.GetUID() || stored.GetAnnotations()["sdo.dev/accepted-paths"] != record {
				t.Fatalf("created acceptance not preserved: %v", stored)
			}
			application := operator.PlanApplicationResult{Patches: []operator.PlanPatch{{Ref: ref, Operation: "create", After: source}}}
			if err := checkWrites(ctx, c, application); err != nil {
				t.Fatal(err)
			}
			if err := c.Patch(ctx, ref, types.MergePatchType, []byte(`{"metadata":{"annotations":{"unrelated":"changed"}}}`)); err != nil {
				t.Fatal(err)
			}
			if err := checkWrites(ctx, c, application); err == nil {
				t.Fatal("acceptance publication concealed an unrelated write")
			}
			other := created.DeepCopy()
			other.SetUID(types.UID("different-instance"))
			if err := c.RecordAcceptance(ctx, ref, "replacement-record", other); err == nil {
				t.Fatal("different instance inherited acceptance")
			}
			stored, err = c.Get(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			if stored.GetAnnotations()["sdo.dev/accepted-paths"] != record {
				t.Fatal("rejected identity changed acceptance")
			}
		})
	}
}
