package operatorcli

import (
	"reflect"
	"strings"
	"testing"

	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/deployment"
)

func operateTestConfig(registryHost string) OperateConfig {
	var config OperateConfig
	config.Lifecycle.Namespace = "demo"
	config.ApplicationRoot = "/repo/application"
	config.RepositoryMount, config.WorktreeRoot = "/repo", "/repo/worktrees"
	config.BuildKitImage, config.RegistryPushSecret = "buildkit@sha256:"+strings.Repeat("a", 64), "push"
	config.Install.RegistryHost = registryHost
	config.Install.BuildBudgetSeconds = 60
	return config
}

func operateTestDeployer(t *testing.T, config OperateConfig) deployment.Deployer {
	t.Helper()
	deployer, err := operateIdentity(config).deployer(fake.NewSimpleClientset(), deployTarget{
		Namespace: config.Lifecycle.Namespace, ControlNamespace: "sdo-system", AcceptedRoot: config.ApplicationRoot,
		ProposalRoot: config.WorktreeRoot, RepositoryMount: config.RepositoryMount, RepositoryPVC: "repo",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return deployer
}

// The deployer and the agent's offer come from the same full catalogs, so
// every built-in renderer (helm included) is both offered and deployable.
func TestOperateDeployerDeploysEveryBuiltInRenderer(t *testing.T) {
	config := operateTestConfig("registry.local:5000")
	deployer := operateTestDeployer(t, config)
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
	deployer := operateTestDeployer(t, operateTestConfig(""))
	if deployer.Environment != nil {
		t.Fatalf("deployer environment = %+v, want unknown (renderer defaults) when none is discovered", deployer.Environment)
	}
	if got := deployer.Images.IDs(); !reflect.DeepEqual(got, []string{"prebuilt"}) {
		t.Fatalf("without a push registry the deployer has only prebuilt images, got %v", got)
	}
}

func TestDeployerProposalsMustBeOnTheRepositoryMount(t *testing.T) {
	config := operateTestConfig("registry.local:5000")
	_, err := operateIdentity(config).deployer(fake.NewSimpleClientset(), deployTarget{Namespace: "demo", ControlNamespace: "sdo-system", AcceptedRoot: config.ApplicationRoot, ProposalRoot: "/elsewhere", RepositoryMount: "/repo", RepositoryPVC: "repo"}, nil)
	if err == nil {
		t.Fatal("proposal root outside the repository mount accepted")
	}
}
