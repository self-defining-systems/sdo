package operatorcli

import (
	"strings"
	"testing"
)

func TestInstallerNativeArgumentsProduceAtomicTypedBrokerLaunch(t *testing.T) {
	args := []string{"controller", "--app", "/workspace/application", "--namespace", "demo", "--application", "demo", "--responder-image", "responder:test", "--prober-image", "controller:test", "--repository-pvc", "repository", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--worktree-root", "/workspace/worktrees", "--response-timeout", "60s", "--verification-timeout", "120s", "--validator-image", "validator:test", "--model", "model", "--reflection-session", "resume", "--reflection-guidance", "generalize-spec", "--force-reflection", "--healthy-baseline", "--max-follow-ups", "2", "--max-response-loops", "3", "--follow-up-cooldown", "25s", "--deployed-manifest-path", "kubernetes", "--supervise", "--broker-mode=native", "--worker-mount-mode=proposal-only"}
	config, supervise, err := ParseControllerLaunch(args)
	if err != nil {
		t.Fatal(err)
	}
	launch := config.Controller
	if !supervise || launch.GetBrokerMode() != "native" || launch.GetWorkerMountMode() != "proposal-only" || launch.GetValidatorImage() != "validator:test" || launch.GetResponderModel() != "model" || launch.GetAppRoot() != "/workspace/application" || launch.GetReflectionSession() != "resume" || launch.GetReflectionGuidance() != "generalize-spec" || !launch.GetForceReflection() || !launch.GetHealthyBaseline() || launch.GetMaxFollowUps() != 2 || launch.GetMaxResponseLoops() != 3 || launch.GetFollowUpCooldown().AsDuration().Seconds() != 25 || len(launch.GetDeployedManifestPaths()) != 1 || launch.GetDeployedManifestPaths()[0] != "kubernetes" {
		t.Fatalf("native mode lost at typed child boundary: %+v", launch)
	}
	for index, arg := range args {
		if arg == "--worker-mount-mode=proposal-only" {
			args[index] = "--worker-mount-mode=linked"
			break
		}
	}
	if _, _, err := ParseControllerLaunch(args); err == nil || !strings.Contains(err.Error(), "proposal-only") {
		t.Fatalf("unsafe mount accepted: %v", err)
	}
}

func TestBoundedNativeControllerLaunch(t *testing.T) {
	args := []string{"controller", "--app", "/workspace/application", "--namespace", "demo", "--responder-image", "responder:test", "--repository-pvc", "repository", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--worktree-root", "/workspace/worktrees", "--validator-image", "validator:test", "--model", "model", "--broker-mode=native", "--worker-mount-mode=proposal-only", "--duration", "30s", "--exit-after-closure", "--repair-policy", "recorded-actions"}
	config, supervise, err := ParseControllerLaunch(args)
	if err == nil && config.Controller.GetReflectionSession() != "resume" {
		t.Fatalf("default reflection mode: %q", config.Controller.GetReflectionSession())
	}
	if err != nil || supervise || config.Controller.GetDuration().AsDuration().Seconds() != 30 || !config.Controller.GetExitAfterClosure() || config.Controller.GetRepairPolicy() != "recorded-actions" {
		t.Fatalf("bounded native launch rejected: %+v %v %v", config.Controller, supervise, err)
	}
	args = append(args, "--supervise")
	if _, _, err := ParseControllerLaunch(args); err == nil {
		t.Fatal("bounded supervision accepted")
	}
	args = args[:len(args)-1]
	args[len(args)-1] = "unknown"
	if _, _, err := ParseControllerLaunch(args); err == nil {
		t.Fatal("invalid repair policy accepted")
	}
}
