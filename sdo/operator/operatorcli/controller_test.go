package operatorcli

import (
	"strings"
	"testing"
)

func TestInstallerNativeArgumentsProduceAtomicTypedBrokerLaunch(t *testing.T) {
<<<<<<< HEAD
	args := []string{"controller", "--toolchain-image", "controller:compiler", "--app", "/workspace/application", "--namespace", "demo", "--application", "demo", "--responder-image", "responder:test", "--repository-pvc", "repository", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--worktree-root", "/workspace/worktrees", "--response-timeout", "60s", "--verification-timeout", "120s", "--repair-policy", "recorded-actions", "--model", "model", "--reflection-session", "resume", "--reflection-guidance", "generalize-spec", "--force-reflection", "--healthy-baseline", "--max-follow-ups", "2", "--max-response-loops", "3", "--follow-up-cooldown", "25s", "--deployed-manifest-path", "kubernetes", "--program-fast-path", "--program-trust-threshold", "4", "--drift-tokens-per-hour", "250000", "--broker-mode=native", "--worker-mount-mode=proposal-only"}
	config, err := ParseControllerLaunch(args)
=======
	args := []string{"controller", "--operator-root", "..", "--toolchain-image", "controller:compiler", "--app", "/workspace/application", "--namespace", "demo", "--application", "demo", "--responder-image", "responder:test", "--prober-image", "controller:test", "--repository-pvc", "repository", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--worktree-root", "/workspace/worktrees", "--response-timeout", "60s", "--verification-timeout", "120s", "--repair-policy", "recorded-actions", "--validator-image", "validator:test", "--model", "model", "--reflection-session", "resume", "--reflection-guidance", "generalize-spec", "--force-reflection", "--healthy-baseline", "--max-response-loops", "3", "--deployed-manifest-path", "kubernetes", "--supervise", "--broker-mode=native", "--worker-mount-mode=proposal-only"}
	config, supervise, err := ParseControllerLaunch(args)
>>>>>>> origin/vic-lsh/dev/sdo-improve
	if err != nil {
		t.Fatal(err)
	}
	launch := config.Controller
<<<<<<< HEAD
	if _, err := ParseControllerLaunch(append(append([]string(nil), args...), "--memory-host")); err == nil {
		t.Fatal("removed memory-host flag accepted")
	}
	configured, configErr := ParseControllerLaunch(append(append([]string(nil), args...), "--program-min-duration", "3s"))
	if configErr != nil || configured.Controller.GetProgramMinDuration().AsDuration().Seconds() != 3 {
		t.Fatalf("program persistence did not reach the typed launch: %v", configErr)
	}
	if launch.GetBrokerMode() != "native" || launch.GetWorkerMountMode() != "proposal-only" || launch.GetResponderModel() != "model" || launch.GetAppRoot() != "/workspace/application" || launch.GetReflectionSession() != "resume" || launch.GetReflectionGuidance() != "generalize-spec" || !launch.GetForceReflection() || !launch.GetHealthyBaseline() || launch.GetMaxFollowUps() != 2 || launch.GetMaxResponseLoops() != 3 || !launch.GetProgramFastPath() || launch.GetProgramTrustThreshold() != 4 || launch.GetDriftTokensPerHour() != 250000 || launch.GetFollowUpCooldown().AsDuration().Seconds() != 25 || len(launch.GetDeployedManifestPaths()) != 1 || launch.GetDeployedManifestPaths()[0] != "kubernetes" {
=======
	for _, flag := range []string{"--program-min-duration", "--drift-sweep-interval"} {
		if _, _, err := ParseControllerLaunch(append(append([]string(nil), args...), flag, "3s")); err == nil {
			t.Fatalf("retired proactive flag accepted: %s", flag)
		}
	}
	if !supervise || launch.GetBrokerMode() != "native" || launch.GetWorkerMountMode() != "proposal-only" || launch.GetValidatorImage() != "validator:test" || launch.GetResponderModel() != "model" || launch.GetAppRoot() != "/workspace/application" || launch.GetReflectionSession() != "resume" || launch.GetReflectionGuidance() != "generalize-spec" || !launch.GetForceReflection() || !launch.GetHealthyBaseline() || launch.GetMaxResponseLoops() != 3 || len(launch.GetDeployedManifestPaths()) != 1 || launch.GetDeployedManifestPaths()[0] != "kubernetes" {
>>>>>>> origin/vic-lsh/dev/sdo-improve
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
<<<<<<< HEAD
	args := []string{"controller", "--toolchain-image", "controller:compiler", "--app", "/workspace/application", "--namespace", "demo", "--responder-image", "responder:test", "--repository-pvc", "repository", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--worktree-root", "/workspace/worktrees", "--model", "model", "--broker-mode=native", "--worker-mount-mode=proposal-only", "--duration", "30s", "--exit-after-closure", "--repair-policy", "recorded-actions"}
	config, err := ParseControllerLaunch(args)
=======
	args := []string{"controller", "--operator-root", "..", "--toolchain-image", "controller:compiler", "--app", "/workspace/application", "--namespace", "demo", "--responder-image", "responder:test", "--repository-pvc", "repository", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--worktree-root", "/workspace/worktrees", "--validator-image", "validator:test", "--model", "model", "--broker-mode=native", "--worker-mount-mode=proposal-only", "--duration", "30s", "--exit-after-closure", "--repair-policy", "recorded-actions"}
	config, supervise, err := ParseControllerLaunch(args)
>>>>>>> origin/vic-lsh/dev/sdo-improve
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
<<<<<<< HEAD
	args := []string{"controller", "--app", "/workspace/application", "--namespace", "demo", "--responder-image", "responder:slim", "--repository-pvc", "repository", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--worktree-root", "/workspace/worktrees", "--model", "model", "--broker-mode=native", "--worker-mount-mode=proposal-only"}
	if _, err := ParseControllerLaunch(args); err == nil || !strings.Contains(err.Error(), "--toolchain-image") {
=======
	args := []string{"controller", "--operator-root", "..", "--app", "/workspace/application", "--namespace", "demo", "--responder-image", "responder:slim", "--prober-image", "traffic-only:test", "--repository-pvc", "repository", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--worktree-root", "/workspace/worktrees", "--validator-image", "validator:slim", "--model", "model", "--broker-mode=native", "--worker-mount-mode=proposal-only"}
	if _, _, err := ParseControllerLaunch(args); err == nil || !strings.Contains(err.Error(), "--toolchain-image") {
>>>>>>> origin/vic-lsh/dev/sdo-improve
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
<<<<<<< HEAD
=======

func TestMemoryMigrationRejectsMissingCompilerBeforeCredentials(t *testing.T) {
	config, _, err := ParseControllerLaunch([]string{"controller", "--operator-root", "..", "--toolchain-image", "controller:test", "--app", "/workspace/app", "--namespace", "app", "--responder-image", "responder:slim", "--repository-pvc", "repo", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--worktree-root", "/workspace/worktrees", "--validator-image", "validator:slim", "--model", "model", "--broker-mode=native", "--worker-mount-mode=proposal-only"})
	if err != nil {
		t.Fatal(err)
	}
	config.Controller.ToolchainImage = ""
	if _, err := openMigrationBroker(config.Controller); err == nil || !strings.Contains(err.Error(), "--toolchain-image") {
		t.Fatalf("missing compiler reached migration: %v", err)
	}
}

func TestScriptImageSurvivesTypedControllerLaunch(t *testing.T) {
	args := append(schemaControllerArgs(".."), "--script-image", "script-tools:slim")
	config, _, err := ParseControllerLaunch(args)
	if err != nil {
		t.Fatal(err)
	}
	message := config.Controller.ProtoReflect()
	field := message.Descriptor().Fields().ByName("script_image")
	if field == nil || message.Get(field).String() != "script-tools:slim" {
		t.Fatal("script image lost at typed controller boundary")
	}
}
>>>>>>> origin/vic-lsh/dev/sdo-improve
