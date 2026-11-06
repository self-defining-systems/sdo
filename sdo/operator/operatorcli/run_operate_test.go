package operatorcli

import (
	"reflect"
	"testing"

	"sdo.dev/operator/deployment"
	"sdo.dev/operator/imagesrc/buildkit"
	"sdo.dev/operator/imagesrc/prebuilt"
	"sdo.dev/operator/render"
)

func operateTestConfig(registryHost string) OperateConfig {
	var config OperateConfig
	config.Lifecycle.Namespace = "demo"
	config.ApplicationRoot = "/repo/application"
	config.Install.RegistryHost = registryHost
	config.Install.BuildBudgetSeconds = 60
	return config
}

// The deployer and the agent's offer come from the same full catalogs, so
// every built-in renderer (helm included) is both offered and deployable.
func TestOperateDeployerDeploysEveryBuiltInRenderer(t *testing.T) {
	config := operateTestConfig("registry.local:5000")
	sources, err := operateImageSources(config, buildkit.Builder{AllowedRegistryHost: config.Install.RegistryHost}, &prebuilt.Source{})
	if err != nil {
		t.Fatal(err)
	}
	deployer := operateDeployer(config, sources, deployment.CommandBackend{})
	if got := deployer.Renderers.IDs(); !reflect.DeepEqual(got, []string{"helm", "kustomize", "raw"}) {
		t.Fatalf("renderers = %v", got)
	}
	if got := deployer.Images.IDs(); !reflect.DeepEqual(got, []string{"buildkit", "prebuilt"}) {
		t.Fatalf("image sources = %v", got)
	}
	if deployer.Namespace != "demo" || deployer.AcceptedRoot != config.ApplicationRoot || deployer.TreeDigest == nil {
		t.Fatalf("deployer = %+v", deployer)
	}
}

// The render environment is the legacy registry's (namespace only), so
// evidence accepted before the switch keeps its env digest; the deployment
// package pins that digest (TestDeployerWithTheProductionEnvAcceptsLegacyRegistryEvidence).
func TestOperateDeployerKeepsTheLegacyRenderEnvironment(t *testing.T) {
	config := operateTestConfig("")
	sources, err := operateImageSources(config, buildkit.Builder{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	deployer := operateDeployer(config, sources, deployment.CommandBackend{})
	if !reflect.DeepEqual(deployer.Env, render.Env{}) {
		t.Fatalf("deployer env = %+v, want only the namespace (set from Deployer.Namespace)", deployer.Env)
	}
	if got := deployer.Images.IDs(); !reflect.DeepEqual(got, []string{"prebuilt"}) {
		t.Fatalf("without a push registry the deployer has only prebuilt images, got %v", got)
	}
}
