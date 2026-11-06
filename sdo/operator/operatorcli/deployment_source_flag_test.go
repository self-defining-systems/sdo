//archcheck:ratchet R5 count=2 until=S3-11c
package operatorcli

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/rendered"
)

const kustomizeSourceJSON = `{"rendererId":"kustomize","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.KustomizeRenderConfig","path":"kubernetes"}}`

func existingLaunchArgs(extra ...string) []string {
	return append([]string{
		"install-existing", "--repository", "/synthetic/repo", "--namespace", "shop", "--application", "shop",
		"--controller-image", "example.invalid/controller", "--responder-image", "example.invalid/responder",
		"--repository-pvc", "repo", "--credentials-secret", "creds", "--model", "synthetic",
	}, extra...)
}

func controllerLaunchArgs(extra ...string) []string {
	return append([]string{
		"controller", "--app", "/synthetic/app", "--namespace", "shop", "--responder-image", "example.invalid/responder",
		"--toolchain-image", "example.invalid/controller", "--repository-pvc", "repo", "--repository-mount-path", "/workspace",
		"--credentials-secret", "creds", "--worktree-root", "/workspace/proposals", "--model", "synthetic",
		"--broker-mode", "native", "--worker-mount-mode", "proposal-only",
	}, extra...)
}

func wantKustomizeSource(t *testing.T, got *contracts.DeploymentSource) {
	t.Helper()
	want, err := rendered.ParseDeploymentSource(kustomizeSourceJSON)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("deployment source = %v, want %v", got, want)
	}
}

func TestInstallExistingCarriesTypedDeploymentSource(t *testing.T) {
	config, err := ParseInstallExistingLaunch(existingLaunchArgs("--deployment-source", kustomizeSourceJSON))
	if err != nil {
		t.Fatal(err)
	}
	wantKustomizeSource(t, config.DeploymentSource)
	var forwarded string
	for _, arg := range config.Install.ExtraControllerArgs {
		if value, ok := strings.CutPrefix(arg, "--deployment-source="); ok {
			forwarded = value
		}
	}
	got, err := rendered.ParseDeploymentSource(forwarded)
	if err != nil {
		t.Fatalf("forwarded controller flag %q: %v", forwarded, err)
	}
	wantKustomizeSource(t, got)
}

func TestInstallExistingPathFormBecomesRawSource(t *testing.T) {
	config, err := ParseInstallExistingLaunch(existingLaunchArgs("--deployed-manifest-path", "k8s/a.yaml", "--deployed-manifest-path", "k8s/b.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if config.DeploymentSource.GetRendererId() != "raw" {
		t.Fatalf("source = %v", config.DeploymentSource)
	}
}

func TestInstallExistingRejectsBothSourceFlags(t *testing.T) {
	_, err := ParseInstallExistingLaunch(existingLaunchArgs("--deployment-source", kustomizeSourceJSON, "--deployed-manifest-path", "k8s"))
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("err = %v", err)
	}
}

func TestControllerLaunchCarriesTypedDeploymentSource(t *testing.T) {
	run, err := ParseControllerLaunch(controllerLaunchArgs("--deployment-source", kustomizeSourceJSON))
	if err != nil {
		t.Fatal(err)
	}
	wantKustomizeSource(t, run.Controller.GetDeploymentSource())
	if len(run.Controller.GetDeployedManifestPaths()) != 0 {
		t.Fatalf("paths set beside the source: %v", run.Controller.GetDeployedManifestPaths())
	}
}

func TestControllerLaunchRejectsBothSourceFlags(t *testing.T) {
	_, err := ParseControllerLaunch(controllerLaunchArgs("--deployment-source", kustomizeSourceJSON, "--deployed-manifest-path", "k8s"))
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("err = %v", err)
	}
}
