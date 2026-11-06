package deployment

import (
	"context"
	"errors"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/renderenv"
)

// The receipt records the environment the deployment rendered with, and
// restart verification renders with that recorded environment, not whatever the
// restarted deployer was handed: a cluster upgrade between install and restart
// is not a renderer change.
func TestReceiptRecordsTheEnvironmentAndVerificationUsesIt(t *testing.T) {
	root, source := depRawFixture(t)
	deployer, _ := depDeployer(t, root, &clusterFake{})
	deployer.Environment = &contracts.RenderEnvironment{KubeVersion: "v1.29.4", ApiVersions: []string{"apps/v1", "apps/v1/Deployment", "v1", "v1/ConfigMap"}}
	ctx := context.Background()
	receipt, err := deployer.ApplyAndVerify(ctx, root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	if !renderenv.Same(receipt.Environment, deployer.Environment) || receipt.Environment == deployer.Environment {
		t.Fatalf("receipt environment = %v, want a copy of %v", receipt.Environment, deployer.Environment)
	}
	if receipt.EnvDigest == legacyRegistryEnvDigest {
		t.Fatal("a discovered environment must change the environment digest")
	}
	upgraded := deployer
	upgraded.Environment = &contracts.RenderEnvironment{KubeVersion: "v1.33.0"}
	if _, err := upgraded.VerifyAccepted(ctx, depHead(t, root), depNamespace, receipt); err != nil {
		t.Fatalf("verification must use the recorded environment: %v", err)
	}
}

// Evidence recorded before discovery (no environment) re-verifies under a
// deployer that has discovered one, with the renderer defaults it was made with.
func TestEvidenceWithoutAnEnvironmentVerifiesWithTheDefaults(t *testing.T) {
	root, source := depRawFixture(t)
	deployer, _ := depDeployer(t, root, &clusterFake{})
	ctx := context.Background()
	receipt, err := deployer.ApplyAndVerify(ctx, root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Environment != nil || receipt.EnvDigest != legacyRegistryEnvDigest {
		t.Fatalf("unknown environment must keep the default digest: %v %s", receipt.Environment, receipt.EnvDigest)
	}
	discovered := deployer
	discovered.Environment = &contracts.RenderEnvironment{KubeVersion: "v1.31.2", ApiVersions: []string{"apps/v1", "apps/v1/Deployment", "v1", "v1/ConfigMap"}}
	if _, err := discovered.VerifyAccepted(ctx, depHead(t, root), depNamespace, receipt); err != nil {
		t.Fatalf("old evidence must not report a renderer change: %v", err)
	}
}

// A render that uses an API the recorded discovery does not serve (a chart that
// fell back to a removed apiVersion) stops before anything is applied, with a
// typed error naming the objects.
func TestDeployRefusesObjectsTheRecordedDiscoveryDoesNotServe(t *testing.T) {
	root, source := depRawFixture(t)
	cluster := &clusterFake{}
	deployer, _ := depDeployer(t, root, cluster)
	// Discovery without v1/ConfigMap: the fixture's ConfigMap is unserved.
	deployer.Environment = &contracts.RenderEnvironment{KubeVersion: "v1.30.0", ApiVersions: []string{"apps/v1", "apps/v1/Deployment", "v1"}}
	_, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb))
	var unserved *rendered.UnservedAPIError
	if !errors.As(err, &unserved) || len(unserved.Objects) != 1 || !strings.Contains(unserved.Objects[0], "v1 ConfigMap") {
		t.Fatalf("want UnservedAPIError for the ConfigMap, got %v", err)
	}
}
