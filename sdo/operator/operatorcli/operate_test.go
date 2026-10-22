package operatorcli

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/install"
	"sdo.dev/operator/worker_registry/registrytest"
)

func installConfigForTest() install.Config {
	return install.Config{ResponderImage: "agent:test"}
}

func TestOperateLaunchRequiresSharedProposalVolumeAndTypedWorkers(t *testing.T) {
	args := []string{"operate", "--app", "/workspace/application", "--namespace", "demo", "--application", "demo", "--health-objective", "search works", "--operation-id", "op-1", "--max-attempts", "2", "--controller-image", "controller:test", "--responder-image", "responder:test", "--validator-image", "validator:test", "--repository-pvc", "repository", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--model", "model", "--agent-provider", "codex", "--worktree-root", "/workspace/worktrees", "--buildkit-image", "buildkit@sha256:" + strings.Repeat("a", 64), "--registry-push-secret", "registry-push", "--registry-host", "registry.example:5000", "--registry-insecure", "--healthy-baseline", "--responder-env", "SDO_SREGYM_API_BASE=http://relay", "--late-findings", "pull", "--repair-policy", "recorded-actions", "--reflection-session", "resume", "--reflection-guidance", "generalize-spec", "--response-timeout", "31m", "--verification-timeout", "3m"}
	config, err := ParseOperateLaunch(args)
	if err != nil {
		t.Fatal(err)
	}
	if config.Lifecycle.OperationID != "op-1" || config.Lifecycle.HealthObjective != "search works" || config.ApplicationRoot != "/workspace/application" || !config.Install.RegistryInsecure || !config.Install.HealthyBaseline || config.Install.RegistryHost != "registry.example:5000" || len(config.Install.ResponderEnv) != 1 || config.Install.LateFindings != "pull" || config.Install.RepairPolicy != "recorded-actions" || config.Install.ReflectionSession != "resume" || config.Install.ReflectionGuidance != "generalize-spec" || config.Install.TimeoutSeconds != 1860 || config.Install.VerificationTimeoutSeconds != 180 {
		t.Fatalf("incomplete native operate launch: %+v", config)
	}
	invalidRegistry := append([]string{}, args...)
	invalidRegistry[indexOf(invalidRegistry, "--registry-host")+1] = "registry.example:5000/app"
	if _, err := ParseOperateLaunch(invalidRegistry); err == nil || !strings.Contains(err.Error(), "registry host") {
		t.Fatalf("registry path accepted: %v", err)
	}
	bounded, err := ParseOperateLaunch(append(append([]string{}, args...), "--exit-after-closure"))
	if err != nil || !bounded.Install.ExitAfterClosure || !bounded.Install.WaitForCompletion {
		t.Fatalf("bounded operate launch: %+v %v", bounded.Install, err)
	}
	for index, arg := range args {
		if arg == "/workspace/worktrees" {
			args[index] = "/tmp/worktrees"
			break
		}
	}
	if _, err := ParseOperateLaunch(args); err == nil || !strings.Contains(err.Error(), "shared repository mount") {
		t.Fatalf("proposal outside shared PVC accepted: %v", err)
	}
	args[indexOf(args, "--app")+1] = "/workspace/other"
	args[indexOf(args, "--worktree-root")+1] = "/workspace/worktrees"
	if _, err := ParseOperateLaunch(args); err == nil || !strings.Contains(err.Error(), "application directory") {
		t.Fatalf("noninstaller checkout accepted: %v", err)
	}
}

func indexOf(items []string, value string) int {
	for index, item := range items {
		if item == value {
			return index
		}
	}
	return -1
}

func TestOperateLaunchKeepsAgentRegistrationMountTogether(t *testing.T) {
	args := []string{"operate", "--app", "/workspace/application", "--namespace", "demo", "--application", "demo", "--health-objective", "healthy", "--operation-id", "op-2", "--controller-image", "controller:test", "--responder-image", "agent:test", "--validator-image", "validator:test", "--repository-pvc", "repository", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--model", "model", "--worktree-root", "/workspace/worktrees", "--buildkit-image", "buildkit@sha256:" + strings.Repeat("a", 64), "--registry-push-secret", "registry-push"}
	if _, err := ParseOperateLaunch(append(append([]string{}, args...), "--agent-registration-directory", "/sdo/registrations")); err == nil {
		t.Fatal("accepted registration path without mounted ConfigMap")
	}
	config, err := ParseOperateLaunch(append(args, "--agent-registration-configmap", "agents", "--agent-registration-directory", "/sdo/registrations"))
	if err != nil {
		t.Fatal(err)
	}
	if config.Install.RegistrationConfigMap != "agents" || config.Install.RegistrationDirectory != "/sdo/registrations" {
		t.Fatalf("lost registration mount: %+v", config.Install)
	}
}

func TestBuiltInLifecycleWorkersHaveTypedCapabilities(t *testing.T) {
	registry, err := lifecycleRegistry(OperateConfig{Install: installConfigForTest()}, registrytest.Directory())
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []proto.Message{&contracts.SourceDeploymentRequest{}, &contracts.ArchitectureRequest{}, &contracts.HealthJudgeRequest{}} {
		agent, err := registry.AgentFor(request)
		if err != nil {
			t.Fatal(err)
		}
		task, err := registry.TaskType(agent, request)
		if err != nil {
			t.Fatal(err)
		}
		registration, err := registry.Resolve(agent, task)
		if err != nil || registration.GetKubernetesJob().GetImage() != "agent:test" {
			t.Fatalf("registration %s/%s: %v", agent, task, err)
		}
	}
}

func TestOperateRejectsInstallerCheckoutMismatchBeforeClusterAccess(t *testing.T) {
	_, err := RunOperate(context.Background(), OperateConfig{ApplicationRoot: "/workspace/other", RepositoryMount: "/workspace", WorktreeRoot: "/workspace/worktrees", Install: installConfigForTest()})
	if err == nil || !strings.Contains(err.Error(), "application directory") {
		t.Fatalf("mismatched repository accepted: %v", err)
	}
}
