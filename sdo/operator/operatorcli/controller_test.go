package operatorcli

import (
	"strings"
	"testing"
)

func TestInstallerNativeArgumentsProduceAtomicTypedBrokerLaunch(t *testing.T) {
	args := []string{"controller", "--toolchain-image", "controller:compiler", "--app", "/workspace/application", "--namespace", "demo", "--application", "demo", "--responder-image", "responder:test", "--repository-pvc", "repository", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--worktree-root", "/workspace/worktrees", "--response-timeout", "60s", "--verification-timeout", "120s", "--repair-policy", "recorded-actions", "--model", "model", "--reflection-session", "resume", "--reflection-guidance", "generalize-spec", "--force-reflection", "--healthy-baseline", "--max-follow-ups", "2", "--max-response-loops", "3", "--follow-up-cooldown", "25s", "--deployed-manifest-path", "kubernetes", "--program-fast-path", "--program-trust-threshold", "4", "--drift-tokens-per-hour", "250000", "--broker-mode=native", "--worker-mount-mode=proposal-only"}
	config, err := ParseControllerLaunch(args)
	if err != nil {
		t.Fatal(err)
	}
	launch := config.Controller
	if _, err := ParseControllerLaunch(append(append([]string(nil), args...), "--memory-host")); err == nil {
		t.Fatal("removed memory-host flag accepted")
	}
	configured, configErr := ParseControllerLaunch(append(append([]string(nil), args...), "--program-min-duration", "3s"))
	if configErr != nil || configured.Controller.GetProgramMinDuration().AsDuration().Seconds() != 3 {
		t.Fatalf("program persistence did not reach the typed launch: %v", configErr)
	}
	if launch.GetBrokerMode() != "native" || launch.GetWorkerMountMode() != "proposal-only" || launch.GetResponderModel() != "model" || launch.GetAppRoot() != "/workspace/application" || launch.GetReflectionSession() != "resume" || launch.GetReflectionGuidance() != "generalize-spec" || !launch.GetForceReflection() || !launch.GetHealthyBaseline() || launch.GetMaxFollowUps() != 2 || launch.GetMaxResponseLoops() != 3 || !launch.GetProgramFastPath() || launch.GetProgramTrustThreshold() != 4 || launch.GetDriftTokensPerHour() != 250000 || launch.GetFollowUpCooldown().AsDuration().Seconds() != 25 || len(launch.GetDeployedManifestPaths()) != 1 || launch.GetDeployedManifestPaths()[0] != "kubernetes" {
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
	args = append(args, "--supervise")
	if _, err := ParseControllerLaunch(args); err == nil {
		t.Fatal("bounded supervision accepted")
	}
	args = args[:len(args)-1]
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
