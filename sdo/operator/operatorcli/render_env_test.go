package operatorcli

import (
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/renderenv"
)

// --render-env reaches the controller launch, and from there every read and
// the upstream deployer; absent is unknown.
func TestControllerRenderEnvFlagReachesTheLaunch(t *testing.T) {
	environment := &contracts.RenderEnvironment{KubeVersion: "v1.31.2", ApiVersions: []string{"apps/v1", "v1"}}
	encoded, err := renderenv.Format(environment)
	if err != nil {
		t.Fatal(err)
	}
	config, _, err := parseControllerLaunch(controllerLaunchArgs("--render-env", encoded))
	if err != nil {
		t.Fatal(err)
	}
	if !renderenv.Same(config.Controller.GetRenderEnvironment(), environment) {
		t.Fatalf("launch environment = %v, want %v", config.Controller.GetRenderEnvironment(), environment)
	}
	plain, _, err := parseControllerLaunch(controllerLaunchArgs())
	if err != nil || plain.Controller.GetRenderEnvironment() != nil {
		t.Fatalf("no flag = %v, %v; want unknown", plain.Controller.GetRenderEnvironment(), err)
	}
	if _, _, err := parseControllerLaunch(controllerLaunchArgs("--render-env", "{bad")); err == nil {
		t.Fatal("malformed --render-env accepted")
	}
}

func TestUpstreamDeployerCarriesTheLaunchEnvironment(t *testing.T) {
	config := operateTestConfig("registry.local:5000")
	environment := &contracts.RenderEnvironment{KubeVersion: "v1.31.2"}
	deployer, err := operateIdentity(config).deployer(nil, deployTarget{
		Namespace: "demo", ControlNamespace: "sdo-system", AcceptedRoot: config.ApplicationRoot,
		WorkspaceRoot: config.WorktreeRoot, RepositoryMount: config.RepositoryMount, RepositoryPVC: "repo",
	}, environment, nil)
	if err != nil {
		t.Fatal(err)
	}
	if deployer.Environment != environment {
		t.Fatalf("deployer environment = %v, want the one it was given", deployer.Environment)
	}
}
