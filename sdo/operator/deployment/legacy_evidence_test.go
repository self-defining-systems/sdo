package deployment

import (
	"context"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/render/raw"
)

var (
	_ lifecycle.SourceReadinessVerifier = Deployer{}
	_ Applier                           = CommandBackend{}
)

// Digests the legacy strategy registry (deleted in S3-13b) recorded for the
// depRawFixture tree deployed as manifest paths ["k8s"] into namespace "demo"
// with render.Env{Namespace: "demo"}, its only production environment. They
// were captured by running that registry before it was deleted.
const (
	legacyRegistryEnvDigest    = "5d135dd6a815d648e33f4cdb15266a1fb46693604f22f7535b2de205968ac4c0"
	legacyRegistryRenderDigest = "2125c7471ddba75fef39d7ea09f1a23b23294ca2a4fcc939df10916eb22a44af"
)

// Digests of evidence written by the legacy registry (S3-13b; effective tree
// fields added by S5-3b) restart
// under the Deployer: with the production render environment (no install
// configuration beyond the namespace) the Deployer binds the same digests and
// re-verifies that evidence instead of reporting a renderer change.
func TestDeployerWithTheProductionEnvAcceptsLegacyRegistryEvidence(t *testing.T) {
	root, source := depRawFixture(t)
	cluster := &clusterFake{}
	deployer, _ := depDeployer(t, root, cluster)
	ctx := context.Background()
	receipt, err := deployer.ApplyAndVerify(ctx, root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.EnvDigest != legacyRegistryEnvDigest || receipt.RenderDigest != legacyRegistryRenderDigest {
		t.Fatalf("deployer binds env %s render %s, legacy evidence has env %s render %s", receipt.EnvDigest, receipt.RenderDigest, legacyRegistryEnvDigest, legacyRegistryRenderDigest)
	}
	// The registry recorded the raw source of its manifest paths.
	legacy := lifecycle.DeploymentReceipt{
		Source: depSource(t, raw.ID, &contracts.RawRenderConfig{Paths: []string{"k8s"}}), BaseTree: receipt.BaseTree, EffectiveTree: receipt.EffectiveTree, PatchesDigest: receipt.PatchesDigest, ObservedRevision: receipt.EffectiveTree,
		RenderDigest: legacyRegistryRenderDigest, EnvDigest: legacyRegistryEnvDigest,
		Images: receipt.Images, ResourceIDs: receipt.ResourceIDs, Ready: true,
	}
	if _, err := deployer.VerifyAccepted(ctx, depHead(t, root), depNamespace, legacy); err != nil {
		t.Fatalf("legacy evidence must re-verify under the deployer: %v", err)
	}
}
