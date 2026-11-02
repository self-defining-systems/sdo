package operatorcli

import (
	"strings"
	"testing"
)

func TestInstallerNativeArgumentsProduceAtomicTypedBrokerLaunch(t *testing.T) {
	args := []string{"controller", "--toolchain-image", "controller:compiler", "--app", "/workspace/application", "--namespace", "demo", "--application", "demo", "--responder-image", "responder:test", "--repository-pvc", "repository", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--worktree-root", "/workspace/worktrees", "--response-timeout", "60s", "--verification-timeout", "120s", "--repair-policy", "recorded-actions", "--model", "model", "--reflection-session", "resume", "--reflection-guidance", "generalize-spec", "--force-reflection", "--healthy-baseline", "--max-response-loops", "3", "--deployed-manifest-path", "kubernetes", "--broker-mode=native", "--worker-mount-mode=proposal-only"}
	config, err := ParseControllerLaunch(args)
	if err != nil {
		t.Fatal(err)
	}
	launch := config.Controller
	if _, err := ParseControllerLaunch(append(append([]string(nil), args...), "--memory-host")); err == nil {
		t.Fatal("removed memory-host flag accepted")
	}
	for _, flag := range []string{"--program-min-duration", "--drift-sweep-interval"} {
		if _, err := ParseControllerLaunch(append(append([]string(nil), args...), flag, "3s")); err == nil {
			t.Fatalf("retired proactive flag accepted: %s", flag)
		}
	}
	if launch.GetBrokerMode() != "native" || launch.GetWorkerMountMode() != "proposal-only" || launch.GetResponderModel() != "model" || launch.GetAppRoot() != "/workspace/application" || launch.GetReflectionSession() != "resume" || launch.GetReflectionGuidance() != "generalize-spec" || !launch.GetForceReflection() || !launch.GetHealthyBaseline() || launch.GetMaxResponseLoops() != 3 || len(launch.GetDeployedManifestPaths()) != 1 || launch.GetDeployedManifestPaths()[0] != "kubernetes" {
		t.Fatalf("native mode lost at typed child boundary: %+v", launch)
	}
	for index, arg := range args {
		if arg == "--worker-mount-mode=proposal-only" {
			args[index] = "--worker-mount-mode=linked"
			break
		}
	}
	if _, err := ParseControllerLaunch(args); err == nil || !strings.Contains(err.Error(), "proposal-only") {
		t.Fatalf("unsafe mount accepted: %v", err)
	}
}

func TestBoundedNativeControllerLaunch(t *testing.T) {
	args := []string{"controller", "--toolchain-image", "controller:compiler", "--app", "/workspace/application", "--namespace", "demo", "--responder-image", "responder:test", "--repository-pvc", "repository", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--worktree-root", "/workspace/worktrees", "--model", "model", "--broker-mode=native", "--worker-mount-mode=proposal-only", "--duration", "30s", "--exit-after-closure", "--repair-policy", "recorded-actions"}
	config, err := ParseControllerLaunch(args)
	if err == nil && config.Controller.GetReflectionSession() != "resume" {
		t.Fatalf("default reflection mode: %q", config.Controller.GetReflectionSession())
	}
	if err != nil || config.Controller.GetDuration().AsDuration().Seconds() != 30 || !config.Controller.GetExitAfterClosure() || config.Controller.GetRepairPolicy() != "recorded-actions" {
		t.Fatalf("bounded native launch rejected: %+v %v", config.Controller, err)
	}
	args[len(args)-1] = "unknown"
	if _, err := ParseControllerLaunch(args); err == nil {
		t.Fatal("invalid repair policy accepted")
	}
}

func TestControllerLaunchRequiresDedicatedToolchainImage(t *testing.T) {
	args := []string{"controller", "--app", "/workspace/application", "--namespace", "demo", "--responder-image", "responder:slim", "--repository-pvc", "repository", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--worktree-root", "/workspace/worktrees", "--model", "model", "--broker-mode=native", "--worker-mount-mode=proposal-only"}
	if _, err := ParseControllerLaunch(args); err == nil || !strings.Contains(err.Error(), "--toolchain-image") {
		t.Fatalf("missing toolchain must fail before building or launching: %v", err)
	}
	config, err := ParseControllerLaunch(append(args, "--toolchain-image", "controller:compiler"))
	if err != nil {
		t.Fatal(err)
	}
	message := config.Controller.ProtoReflect()
	field := message.Descriptor().Fields().ByName("toolchain_image")
	if field == nil || message.Get(field).String() != "controller:compiler" {
		t.Fatal("dedicated compiler image did not reach typed controller launch")
	}
}

func TestControllerBrokerRejectsMissingCompilerBeforeCredentials(t *testing.T) {
	config, err := ParseControllerLaunch([]string{"controller", "--toolchain-image", "controller:test", "--app", "/workspace/app", "--namespace", "app", "--responder-image", "responder:slim", "--repository-pvc", "repo", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--worktree-root", "/workspace/worktrees", "--model", "model", "--broker-mode=native", "--worker-mount-mode=proposal-only"})
	if err != nil {
		t.Fatal(err)
	}
	config.Controller.ToolchainImage = ""
	if _, err := openControllerBroker(config.Controller); err == nil || !strings.Contains(err.Error(), "--toolchain-image") {
		t.Fatalf("missing compiler reached the broker: %v", err)
	}
}

func TestScriptImageSurvivesTypedControllerLaunch(t *testing.T) {
	args := append(schemaControllerArgs(""), "--script-image", "script-tools:slim")
	config, err := ParseControllerLaunch(args)
	if err != nil {
		t.Fatal(err)
	}
	message := config.Controller.ProtoReflect()
	field := message.Descriptor().Fields().ByName("script_image")
	if field == nil || message.Get(field).String() != "script-tools:slim" {
		t.Fatal("script image lost at typed controller boundary")
	}
}

func schemaControllerArgs(root string) []string {
	return []string{"controller", "--toolchain-image", "controller:test", "--app", "/workspace/application", "--namespace", "demo", "--responder-image", "responder:test", "--repository-pvc", "repository", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--worktree-root", "/workspace/worktrees", "--model", "model", "--broker-mode=native", "--worker-mount-mode=proposal-only"}
}
