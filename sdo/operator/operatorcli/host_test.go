package operatorcli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHostLaunchBuildsNativeInstallerContract(t *testing.T) {
	args := []string{
		"host", "--repository", "/tmp/app", "--namespace", "demo", "--control-namespace", "demo-sdo",
		"--application", "demo", "--health-objective", "pods stay healthy", "--operation-id", "op-1",
		"--controller-image", "controller:test", "--responder-image", "agent:test",
		"--buildkit-image", "moby/buildkit@sha256:" + strings.Repeat("a", 64), "--repository-pvc", "repository",
		"--credentials-secret", "agent-credentials", "--registry-push-secret", "registry-push", "--registry-host", "registry.example:5000",
		"--model", "model", "--agent-provider", "codex", "--max-attempts", "2",
		"--responder-env", "SDO_SREGYM_API_BASE=http://relay", "--registry-insecure", "--healthy-baseline", "--late-findings", "pull", "--repair-policy", "recorded-actions", "--reflection-session", "resume", "--reflection-guidance", "generalize-spec", "--max-follow-ups", "2", "--follow-up-cooldown", "25s", "--response-timeout", "31m", "--verification-timeout", "3m",
	}
	config, err := ParseHostLaunch(args)
	if err != nil {
		t.Fatal(err)
	}
	if config.Repository != "/tmp/app" || config.Install.RegistryPushSecret != "registry-push" || config.Install.RegistryHost != "registry.example:5000" || config.MaxAttempts != 2 || config.Install.ControlNamespace != "demo-sdo" || !config.Install.RegistryInsecure || !config.Install.HealthyBaseline || len(config.Install.ResponderEnv) != 1 || config.Install.LateFindings != "pull" || config.Install.RepairPolicy != "recorded-actions" || config.Install.ReflectionSession != "resume" || config.Install.ReflectionGuidance != "generalize-spec" || config.Install.MaxFollowUps != 2 || config.Install.FollowUpCooldownSeconds != 25 || config.Install.TimeoutSeconds != 1860 || config.Install.VerificationTimeoutSeconds != 180 {
		t.Fatalf("incomplete host launch: %+v", config)
	}
	bounded, err := ParseHostLaunch(append(append([]string{}, args...), "--controller-duration", "30s"))
	if err != nil || bounded.Install.ControllerDurationSeconds != 30 || !bounded.Install.WaitForCompletion {
		t.Fatalf("bounded host launch: %+v %v", bounded.Install, err)
	}
	args[indexOf(args, "--registry-push-secret")+1] = "agent-credentials"
	if _, err := ParseHostLaunch(args); err == nil || !strings.Contains(err.Error(), "separate") {
		t.Fatalf("shared push credentials accepted: %v", err)
	}
	args[indexOf(args, "--registry-push-secret")+1] = "registry-push"
	args[indexOf(args, "--registry-host")+1] = "https://registry.example:5000/app"
	if _, err := ParseHostLaunch(args); err == nil || !strings.Contains(err.Error(), "registry host") {
		t.Fatalf("untrusted registry authority accepted: %v", err)
	}
}

func TestHostAcceptsGoalFileWithoutPythonCLI(t *testing.T) {
	goal := filepath.Join(t.TempDir(), "goal.md")
	if err := os.WriteFile(goal, []byte("Keep the service healthy\n"), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"host", "--repository", "/tmp/app", "--namespace", "demo", "--application", "demo", "--goal-file", goal, "--operation-id", "op-1", "--controller-image", "controller:test", "--responder-image", "agent:test", "--buildkit-image", "buildkit@sha256:" + strings.Repeat("a", 64), "--repository-pvc", "repo", "--credentials-secret", "agents", "--registry-push-secret", "registry", "--model", "model"}
	launch, err := ParseHostLaunch(args)
	if err != nil {
		t.Fatal(err)
	}
	if launch.Install.ReflectionSession != "resume" {
		t.Fatalf("default reflection mode: %q", launch.Install.ReflectionSession)
	}
	if launch.Objective != "Keep the service healthy" {
		t.Fatalf("goal file lost: %q", launch.Objective)
	}
	if _, err := ParseHostLaunch(append(args, "--health-objective", "other")); err == nil {
		t.Fatal("conflicting goals accepted")
	}
}
