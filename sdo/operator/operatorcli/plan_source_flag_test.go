package operatorcli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

// install-existing takes no deployment source: the repository's files and its
// optional override say how to deploy, and the controller is given no source
// or path flag.
func TestInstallExistingTakesNoSourceFlag(t *testing.T) {
	config, err := ParseInstallExistingLaunch(existingLaunchArgs())
	if err != nil {
		t.Fatal(err)
	}
	for _, arg := range config.Install.ExtraControllerArgs {
		if strings.HasPrefix(arg, "--deployment-source") || strings.HasPrefix(arg, "--deployed-manifest-path") {
			t.Fatalf("controller argument %q: the controller reads the accepted tree", arg)
		}
	}
	for _, flag := range []string{"--deployment-source", "--deployed-manifest-path"} {
		if _, err := ParseInstallExistingLaunch(existingLaunchArgs(flag, kustomizeSourceJSON)); err == nil {
			t.Fatalf("the retired %s flag is accepted", flag)
		}
	}
}

// install-existing names the branch SDO commits to and deploys from, as host and operate do, so engineers' pushes to
// that remote reach a lane installed from existing memory.
func TestInstallExistingTakesTheSourceBranchRemote(t *testing.T) {
	config, err := ParseInstallExistingLaunch(existingLaunchArgs("--source-branch-url", "git://up.invalid/app.git", "--source-branch", "sdo-source"))
	if err != nil {
		t.Fatal(err)
	}
	if got := config.Install.MemoryUpstreamURL + " " + config.Install.MemoryUpstreamBranch; got != "git://up.invalid/app.git sdo-source" {
		t.Fatalf("source branch = %q", got)
	}
	plain, err := ParseInstallExistingLaunch(existingLaunchArgs())
	if err != nil || plain.Install.MemoryUpstreamURL != "" {
		t.Fatalf("no flag: %v %q", err, plain.Install.MemoryUpstreamURL)
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

// A controller on a repository whose override does not parse does not start;
// one without an override is not refused for it (the source is derived).
func TestRunControllerRefusesAnOverrideThatDoesNotParse(t *testing.T) {
	args := controllerLaunchArgs()
	args[2] = t.TempDir()
	err := RunController(context.Background(), args)
	if errors.Is(err, planfile.ErrAbsent) || (err != nil && strings.Contains(err.Error(), "plan.yaml")) {
		t.Fatalf("RunController without an override = %v", err)
	}
	if err := os.MkdirAll(filepath.Join(args[2], ".sdo", "deployment"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(args[2], ".sdo", "deployment", "plan.yaml"), []byte("colour: red\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := RunController(context.Background(), args); err == nil || !strings.Contains(err.Error(), "controller cannot start") {
		t.Fatalf("RunController with a bad override = %v", err)
	}
}
