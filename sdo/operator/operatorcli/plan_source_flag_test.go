package operatorcli

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/planfile"
)

const kustomizeSourceJSON = `{"rendererId":"kustomize","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.KustomizeRenderConfig","path":"kubernetes"}}`

func existingLaunchArgs(extra ...string) []string {
	return append([]string{
		"install-existing", "--repository", "/synthetic/repo", "--namespace", "shop", "--application", "shop",
		"--controller-image", "example.invalid/controller", "--responder-image", "example.invalid/responder",
		"--repository-pvc", "repo", "--credentials-secret", "creds", "--config", testModelConfig,
	}, extra...)
}

func controllerLaunchArgs(extra ...string) []string {
	return append([]string{
		"controller", "--app", "/synthetic/app", "--namespace", "shop", "--responder-image", "example.invalid/responder",
		"--toolchain-image", "example.invalid/controller", "--repository-pvc", "repo", "--repository-mount-path", "/workspace",
		"--credentials-secret", "creds", "--worktree-root", "/workspace/proposals",
		"--broker-mode", "native", "--worker-mount-mode", "proposal-only",
	}, extra...)
}

func wantKustomizeSource(t *testing.T, got *contracts.DeploymentSource) {
	t.Helper()
	want, err := parseDeploymentSourceFlag(kustomizeSourceJSON)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("deployment source = %v, want %v", got, want)
	}
}

// install-existing takes the source as input to plan.yaml; the controller it
// installs is never given a source or path flag.
func TestInstallExistingParsesTheSourceAndForwardsNoFlag(t *testing.T) {
	config, err := ParseInstallExistingLaunch(existingLaunchArgs("--deployment-source", kustomizeSourceJSON))
	if err != nil {
		t.Fatal(err)
	}
	wantKustomizeSource(t, config.DeploymentSource)
	for _, arg := range config.Install.ExtraControllerArgs {
		if strings.HasPrefix(arg, "--deployment-source") || strings.HasPrefix(arg, "--deployed-manifest-path") {
			t.Fatalf("controller argument %q: the controller reads plan.yaml", arg)
		}
	}
	if _, err := ParseInstallExistingLaunch(existingLaunchArgs("--deployment-source", "not json")); err == nil || !strings.Contains(err.Error(), "invalid deployment source") {
		t.Fatalf("malformed source: err = %v", err)
	}
	if _, err := ParseInstallExistingLaunch(existingLaunchArgs("--deployed-manifest-path", "k8s")); err == nil {
		t.Fatal("the retired path flag is accepted")
	}
}

func TestControllerLaunchRejectsTheRetiredSourceFlags(t *testing.T) {
	if _, err := ParseControllerLaunch(controllerLaunchArgs()); err != nil {
		t.Fatalf("launch without a source flag: %v", err)
	}
	for _, flag := range []string{"--deployment-source", "--deployed-manifest-path"} {
		if _, err := ParseControllerLaunch(controllerLaunchArgs(flag, kustomizeSourceJSON)); err == nil {
			t.Fatalf("controller accepts the retired %s", flag)
		}
	}
}

// A controller on a repository without .sdo/deployment/plan.yaml does not
// start: it names how to write one.
func TestRunControllerRequiresAPlanInTheAcceptedTree(t *testing.T) {
	args := controllerLaunchArgs()
	args[2] = t.TempDir()
	err := RunController(context.Background(), args)
	if !errors.Is(err, planfile.ErrAbsent) || !strings.Contains(err.Error(), "install-existing --deployment-source") {
		t.Fatalf("RunController without a plan = %v", err)
	}
}
