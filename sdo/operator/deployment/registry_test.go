//archcheck:ratchet R5 count=1 until=S3-13b
package deployment

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/raw"
)

var (
	_ lifecycle.SourceReadinessVerifier = Deployer{}
	_ lifecycle.SourceReadinessVerifier = (*Registry)(nil)
)

// regFixture registers a synthetic raw strategy whose cluster is always ready
// at the deployed tree, so the test exercises only the registry's contract.
func regFixture(t *testing.T, root string, env render.Env) *Registry {
	t.Helper()
	image := "web@" + synthDigest
	observe := func(tree string) Observation {
		return Observation{Revision: tree, Images: []string{image}, ResourceIDs: []string{depRawWeb}, Ready: true}
	}
	registry, err := NewRegistry(Registration{
		ID: "raw-manifests-v1", Config: &contracts.RawManifestStrategy{},
		Apply: func(_ context.Context, _, tree string, _ proto.Message) (Observation, error) { return observe(tree), nil },
		Verify: func(_ context.Context, _, _ string, prior lifecycle.DeploymentReceipt) (Observation, error) {
			return observe(prior.BaseTree), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry.AcceptedRoot, registry.Renderers, registry.Env = root, depRenderers(t), env
	return registry
}

func legacyRawPlan(t *testing.T, paths ...string) *contracts.DeploymentPlan {
	t.Helper()
	config, err := anypb.New(&contracts.RawManifestStrategy{ManifestPaths: paths, ExpectedWorkloadIds: []string{depRawWeb}})
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.DeploymentPlan{StrategyId: "raw-manifests-v1", StrategyConfig: config}
}

// The legacy registry binds the same source and digests the render-seam
// Deployer binds for the same tree and environment, so switching deployers
// (S3-13b) does not read as a renderer change.
func TestRegistryReceiptBindsSourceAndTheDeployersDigests(t *testing.T) {
	root, source := depRawFixture(t)
	deployer, _ := depDeployer(t, root, &clusterFake{})
	want, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	registry := regFixture(t, root, render.Env{Namespace: depNamespace, Release: deployer.Env.Release})
	got, err := registry.ApplyAndVerify(context.Background(), root, legacyRawPlan(t, "k8s"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Source.GetRendererId() != raw.ID || !proto.Equal(got.Source, source) {
		t.Fatalf("receipt source = %v, want %v", got.Source, source)
	}
	if got.BaseTree != want.BaseTree || got.RenderDigest != want.RenderDigest || got.EnvDigest != want.EnvDigest {
		t.Fatalf("registry binds %s/%s/%s, deployer %s/%s/%s", got.BaseTree, got.RenderDigest, got.EnvDigest, want.BaseTree, want.RenderDigest, want.EnvDigest)
	}
}

func TestRegistryVerifyAcceptedRerendersTheRecordedSource(t *testing.T) {
	root, _ := depRawFixture(t)
	env := render.Env{Namespace: depNamespace}
	registry := regFixture(t, root, env)
	ctx := context.Background()
	receipt, err := registry.ApplyAndVerify(ctx, root, legacyRawPlan(t, "k8s"))
	if err != nil {
		t.Fatal(err)
	}
	commit := depHead(t, root)
	if _, err := registry.VerifyAccepted(ctx, commit, depNamespace, receipt); err != nil {
		t.Fatalf("verify right after deploy: %v", err)
	}

	changed := regFixture(t, root, render.Env{Namespace: depNamespace, Release: "other"})
	var renderer *RendererChangedError
	if _, err := changed.VerifyAccepted(ctx, commit, depNamespace, receipt); !errors.As(err, &renderer) || renderer.What != "env" {
		t.Fatalf("err = %v, want RendererChangedError on env", err)
	}
	narrowed := receipt
	narrowed.Source = depSource(t, raw.ID, &contracts.RawRenderConfig{Paths: []string{"k8s/web.yaml"}})
	if _, err := registry.VerifyAccepted(ctx, commit, depNamespace, narrowed); !errors.As(err, &renderer) || renderer.What != "render" {
		t.Fatalf("err = %v, want RendererChangedError on render", err)
	}
	unsourced := receipt
	unsourced.Source = nil
	if _, err := registry.VerifyAccepted(ctx, commit, depNamespace, unsourced); err == nil {
		t.Fatal("receipt without a deployment source re-verified")
	}
	if _, err := registry.VerifyAccepted(ctx, commit, "elsewhere", receipt); err == nil {
		t.Fatal("namespace mismatch accepted")
	}
}

func TestRegistryRejectsAnUnrenderableSourceBeforeApplying(t *testing.T) {
	root, _ := depRawFixture(t)
	applied := false
	registry, err := NewRegistry(Registration{
		ID: "raw-manifests-v1", Config: &contracts.RawManifestStrategy{},
		Apply: func(context.Context, string, string, proto.Message) (Observation, error) {
			applied = true
			return Observation{}, errors.New("unreachable")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry.Renderers, registry.Env = depRenderers(t), render.Env{Namespace: depNamespace}
	if _, err := registry.ApplyAndVerify(context.Background(), root, legacyRawPlan(t, "missing")); err == nil || applied {
		t.Fatalf("err = %v, applied = %v; an unrenderable source must fail before any cluster effect", err, applied)
	}
}
