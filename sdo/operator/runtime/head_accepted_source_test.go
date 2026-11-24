package runtime

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type cachedRevisionSource map[string]*unstructured.Unstructured

func (s cachedRevisionSource) AcceptedManifest(_ context.Context, revision string, _ sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return s[revision].DeepCopy(), nil
}

func (s cachedRevisionSource) Revision(ctx context.Context, ref sdk.ObjectRef, revision string) (*unstructured.Unstructured, error) {
	return s.AcceptedManifest(ctx, revision, ref)
}

func TestHeadAcceptanceRebindsSourceRepairWithoutRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	config := testControllerConfig()
	config.SourceCommit, config.DeployedCommit = "v1", "v1"
	config.ExternalIncidentsOnly = true
	c, err := NewController(config, nil, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	accepted := "v1"
	c.SetHeadPoller(&HeadPoller{AcceptedRevision: func(context.Context) (string, error) { return accepted, nil }})
	c.head.nextAt = now.Add(time.Hour)
	var invalidations int
	c.invalidateSourceCache = func() { invalidations++ }
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: config.Namespace, Name: "app"}
	manifest := func(image, field string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": ref.APIVersion, "kind": ref.Kind,
			"metadata": map[string]any{"namespace": ref.Namespace, "name": ref.Name},
			"data":     map[string]any{"release": image, "configuration": field},
		}}
	}
	source := cachedRevisionSource{"v1": manifest("old", "v1-setting"), "v2": manifest("new", "v2-setting")}
	warm, err := source.AcceptedManifest(ctx, c.CurrentAcceptedCommit(), ref)
	if err != nil || warm.Object["data"].(map[string]any)["release"] != "old" {
		t.Fatalf("initial source: %v %v", warm, err)
	}
	// Head polling published the v2 deployment while this controller stayed up.
	accepted = "v2"
	if err := c.StepEvents(ctx, now.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	request := c.incidentRequest(now, nil)
	observed := manifest("new", "drifted")
	snapshot := sdktest.Snapshot{NamespaceName: config.Namespace, ObjectSnapshotVersion: sdktest.ObjectsOnlySnapshotVersion,
		Objects: []unstructured.Unstructured{*observed}}
	view := LiveReadView{Carrier: inputs.New(snapshot), Source: source, SourceRevision: c.CurrentAcceptedCommit}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil {
		t.Fatal(err)
	}
	release, err := diff.Field("data.release")
	if err != nil || release.Differs() || release.Accepted != "new" {
		t.Fatalf("diagnose read stale source: %+v, %v", release, err)
	}
	executor := PlanExecutor{Source: source}
	fromSource, err := executor.valueInputs(ctx, request).Source(ref)
	if err != nil || !planValuesEqual(fromSource, source["v2"].Object) {
		t.Fatalf("FromSource read stale source: %v, %v", fromSource, err)
	}
	next, err := executor.prepareAction(ctx, request, program.ReconcileFromSource{Ref: ref}, observed)
	if err != nil {
		t.Fatal(err)
	}
	if invalidations != 1 {
		t.Fatalf("accepted deployment invalidated pinned renders %d times, want 1", invalidations)
	}
	if request.SourceCommit != "v2" || request.DeployedCommit != "v2" || !planValuesEqual(next.Object, source["v2"].Object) {
		t.Fatalf("source %q deployment %q repaired to %v, want v2", request.SourceCommit, request.DeployedCommit, next.Object)
	}
}

func TestHeadReceiptInvalidatesRenderOfSameRevision(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	config := testControllerConfig()
	config.SourceCommit, config.DeployedCommit = "v2", "v2"
	c, err := NewController(config, nil, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	accepted := ""
	c.SetHeadPoller(&HeadPoller{AcceptedRevision: func(context.Context) (string, error) { return accepted, nil }})
	invalidations := 0
	c.invalidateSourceCache = func() { invalidations++ }
	if err := c.refreshAcceptedDeployment(ctx); err != nil || invalidations != 0 {
		t.Fatalf("before acceptance: invalidations=%d err=%v", invalidations, err)
	}
	accepted = "v2"
	for range 2 {
		if err := c.refreshAcceptedDeployment(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if invalidations != 1 {
		t.Fatalf("same-revision receipt invalidated %d times, want 1", invalidations)
	}
}
