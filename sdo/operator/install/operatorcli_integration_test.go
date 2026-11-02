package install_test

import (
	"strings"
	"testing"

	"sdo.dev/operator/install"
	"sdo.dev/operator/operatorcli"
)

// This crosses the actual installer Job spec and packaged CLI parser. The
// parser's typed ControllerConfig is what the generated child process reads.
func TestInstallerNativeJobPreservesBrokerAndMountModeInChildConfig(t *testing.T) {
	cfg := install.Config{
		Namespace: "demo", ControlNamespace: "demo-sdo", Application: "demo",
		ControllerImage: "controller:test", ResponderImage: "responder:test",
		RepositoryPVC: "repository", CredentialsSecret: "credentials", Model: "gpt-test",
		AcceptedCommit: strings.Repeat("a", 40), TimeoutSeconds: 60, NativeCommand: []string{"/usr/local/bin/sdo-operator"}, RepairPolicy: "recorded-actions",
	}
	plan, err := install.Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	container := plan.Job.Spec.Template.Spec.Containers[0]
	if len(container.Command) != 1 || container.Command[0] != "/usr/local/bin/sdo-operator" {
		t.Fatalf("native command=%v", container.Command)
	}
<<<<<<< HEAD
	launch, err := operatorcli.ParseControllerLaunch(container.Args)
=======
	launch, supervise, err := operatorcli.ParseControllerLaunch(append(container.Args, "--operator-root", ".."))
>>>>>>> origin/vic-lsh/dev/sdo-improve
	if err != nil {
		t.Fatal(err)
	}
	if launch.Controller.GetBrokerMode() != "native" || launch.Controller.GetWorkerMountMode() != "proposal-only" {
		t.Fatalf("native mode lost at child boundary: %+v", launch.Controller)
	}
	if launch.Controller.GetResponderModel() != "gpt-test" || launch.Controller.GetResponderBackend() != "codex" {
		t.Fatalf("native verifier/responder fields lost: %+v", launch.Controller)
	}
	if launch.Controller.GetControlNamespace() != "demo-sdo" || launch.Controller.GetRepositoryPvc() != "repository" {
		t.Fatalf("installation scope lost: %+v", launch.Controller)
	}
	if launch.Controller.GetRepairPolicy() != "recorded-actions" {
		t.Fatalf("repair policy lost at child boundary: %q", launch.Controller.GetRepairPolicy())
	}
}

func TestInstallerOperateJobParsesAsInClusterLifecycle(t *testing.T) {
	cfg := install.Config{Namespace: "demo", ControlNamespace: "demo-sdo", Application: "demo", ControllerImage: "controller:test", ResponderImage: "responder:test", RepositoryPVC: "repository", CredentialsSecret: "credentials", Model: "gpt-test", AcceptedCommit: strings.Repeat("a", 40), TimeoutSeconds: 60, NativeCommand: []string{"/usr/local/bin/sdo-operator"}, RegistrationConfigMap: "sdo-agents", BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "registry-push"}
	job, err := install.OperateJob(cfg, "pods stay healthy", "operation-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	container := job.Spec.Template.Spec.Containers[0]
	launch, err := operatorcli.ParseOperateLaunch(container.Args)
	if err != nil {
		t.Fatal(err)
	}
	if launch.Lifecycle.OperationID != "operation-1" || launch.Lifecycle.HealthObjective != "pods stay healthy" || launch.Lifecycle.MaxAttempts != 3 {
		t.Fatalf("lifecycle config=%+v", launch.Lifecycle)
	}
	if launch.ApplicationRoot != "/workspace/application" || launch.WorktreeRoot != "/workspace/worktrees" {
		t.Fatalf("operate launch=%+v", launch)
	}
	if launch.RegistrationDirectory != "/sdo/registrations" {
		t.Fatalf("registration directory=%q", launch.RegistrationDirectory)
	}
}

func TestInstallerOperateJobCarriesBenchmarkTransportAndRegistrySetting(t *testing.T) {
	cfg := install.Config{Namespace: "demo", Application: "demo", ControllerImage: "controller:test", ResponderImage: "responder:test", RepositoryPVC: "repository", CredentialsSecret: "credentials", Model: "gpt-test", AcceptedCommit: strings.Repeat("a", 40), TimeoutSeconds: 60, VerificationTimeoutSeconds: 45, RepairPolicy: "recorded-actions", NativeCommand: []string{"/usr/local/bin/sdo-operator"}, BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "registry-push", RegistryHost: "registry.demo-sdo.svc.cluster.local:5000", ResponderEnv: []string{"SDO_SREGYM_API_BASE=http://bridge:8000"}, RegistryInsecure: true}
	job, err := install.OperateJob(cfg, "healthy", "operation-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	args := job.Spec.Template.Spec.Containers[0].Args
	want := []string{"--responder-env", "SDO_SREGYM_API_BASE=http://bridge:8000", "--registry-insecure", "--registry-host", "registry.demo-sdo.svc.cluster.local:5000", "--repair-policy", "recorded-actions", "--response-timeout", "60s", "--verification-timeout", "45s"}
	for _, item := range want {
		if !contains(args, item) {
			t.Fatalf("operate Job lost %q: %v", item, args)
		}
	}
}

func TestNativeControllerLateFindingsPullUsesNarrowWorkerSetting(t *testing.T) {
	cfg := install.Config{Namespace: "demo", Application: "demo", ControllerImage: "controller:test", ResponderImage: "responder:test", RepositoryPVC: "repository", CredentialsSecret: "credentials", Model: "gpt-test", AcceptedCommit: strings.Repeat("a", 40), TimeoutSeconds: 60, NativeCommand: []string{"/usr/local/bin/sdo-operator"}, LateFindings: "pull"}
	plan, err := install.Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	args := plan.Job.Spec.Template.Spec.Containers[0].Args
	if !contains(args, "--responder-env=SDO_LATE_FINDINGS=pull") || contains(args, "--broker-arg=--late-findings") {
		t.Fatalf("native late findings did not use narrow worker setting: %v", args)
	}
	cfg.ResponderEnv = []string{"SDO_LATE_FINDINGS=off"}
	if _, err := install.Resources(cfg); err == nil || !strings.Contains(err.Error(), "duplicate SDO_LATE_FINDINGS") {
		t.Fatalf("conflicting late findings setting accepted: %v", err)
	}
}

func TestOperateRejectsRegistryHostWithSchemeOrPath(t *testing.T) {
	cfg := install.Config{Namespace: "demo", Application: "demo", ControllerImage: "controller:test", ResponderImage: "responder:test", RepositoryPVC: "repository", CredentialsSecret: "credentials", Model: "gpt-test", AcceptedCommit: strings.Repeat("a", 40), TimeoutSeconds: 60, NativeCommand: []string{"/usr/local/bin/sdo-operator"}, BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "registry-push"}
	for _, invalid := range []string{"http://registry:5000", "registry:5000/app", "Registry:5000", "registry:0"} {
		cfg.RegistryHost = invalid
		if _, err := install.OperateJob(cfg, "healthy", "operation-1", 1); err == nil {
			t.Fatalf("accepted invalid registry host %q", invalid)
		}
	}
}

func TestNativeReflectionSessionIsForwardedToOperateAndController(t *testing.T) {
	cfg := install.Config{Namespace: "demo", Application: "demo", ControllerImage: "controller:test", ResponderImage: "responder:test", RepositoryPVC: "repository", CredentialsSecret: "credentials", Model: "gpt-test", AcceptedCommit: strings.Repeat("a", 40), TimeoutSeconds: 60, NativeCommand: []string{"/usr/local/bin/sdo-operator"}, BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "registry-push"}
	job, err := install.OperateJob(cfg, "healthy", "operation-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(job.Spec.Template.Spec.Containers[0].Args, "--reflection-session") || !contains(job.Spec.Template.Spec.Containers[0].Args, "resume") {
		t.Fatalf("operate Job lost reflection mode: %v", job.Spec.Template.Spec.Containers[0].Args)
	}
	plan, err := install.Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(plan.Job.Spec.Template.Spec.Containers[0].Args, "--reflection-session") || !contains(plan.Job.Spec.Template.Spec.Containers[0].Args, "resume") {
		t.Fatalf("controller Job lost reflection mode: %v", plan.Job.Spec.Template.Spec.Containers[0].Args)
	}
	cfg.ReflectionSession = "fresh"
	fresh, err := install.OperateJob(cfg, "healthy", "operation-2", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(fresh.Spec.Template.Spec.Containers[0].Args, "fresh") {
		t.Fatalf("fresh reflection mode lost: %v", fresh.Spec.Template.Spec.Containers[0].Args)
	}
}

func TestBoundedNativeInstallCarriesDurationWithoutSupervisor(t *testing.T) {
	cfg := install.Config{Namespace: "demo", Application: "demo", ControllerImage: "controller:test", ResponderImage: "responder:test", RepositoryPVC: "repository", CredentialsSecret: "credentials", Model: "gpt-test", AcceptedCommit: strings.Repeat("a", 40), TimeoutSeconds: 60, NativeCommand: []string{"/usr/local/bin/sdo-operator"}, BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "registry-push", ControllerDurationSeconds: 120, WaitForCompletion: true}
	plan, err := install.Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	controllerArgs := plan.Job.Spec.Template.Spec.Containers[0].Args
	if !contains(controllerArgs, "--duration") || !contains(controllerArgs, "120s") || contains(controllerArgs, "--supervise") {
		t.Fatalf("bounded controller args=%v", controllerArgs)
	}
	operate, err := install.OperateJob(cfg, "healthy", "operation-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	operateArgs := operate.Spec.Template.Spec.Containers[0].Args
	if !contains(operateArgs, "--controller-duration") || !contains(operateArgs, "120s") {
		t.Fatalf("bounded operate args=%v", operateArgs)
	}
}

func TestHealthyBaselineIsForwardedAcrossOperateAndControllerJobs(t *testing.T) {
	cfg := install.Config{Namespace: "demo", Application: "demo", ControllerImage: "controller:test", ResponderImage: "responder:test", RepositoryPVC: "repository", CredentialsSecret: "credentials", Model: "gpt-test", AcceptedCommit: strings.Repeat("a", 40), TimeoutSeconds: 60, NativeCommand: []string{"/usr/local/bin/sdo-operator"}, BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "registry-push", HealthyBaseline: true}
	operate, err := install.OperateJob(cfg, "healthy", "operation-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(operate.Spec.Template.Spec.Containers[0].Args, "--healthy-baseline") {
		t.Fatal("operate Job lost healthy baseline setting")
	}
	operateLaunch, err := operatorcli.ParseOperateLaunch(operate.Spec.Template.Spec.Containers[0].Args)
	if err != nil || !operateLaunch.Install.HealthyBaseline {
		t.Fatalf("operate child lost healthy baseline: %+v, %v", operateLaunch.Install, err)
	}
	plan, err := install.Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(plan.Job.Spec.Template.Spec.Containers[0].Args, "--healthy-baseline") {
		t.Fatal("controller Job lost healthy baseline setting")
	}
<<<<<<< HEAD
	controllerLaunch, err := operatorcli.ParseControllerLaunch(plan.Job.Spec.Template.Spec.Containers[0].Args)
=======
	controllerLaunch, _, err := operatorcli.ParseControllerLaunch(append(plan.Job.Spec.Template.Spec.Containers[0].Args, "--operator-root", ".."))
>>>>>>> origin/vic-lsh/dev/sdo-improve
	if err != nil || !controllerLaunch.Controller.GetHealthyBaseline() {
		t.Fatalf("controller child lost healthy baseline: %+v, %v", controllerLaunch.Controller, err)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestMetricsURLIsForwardedAcrossOperateAndControllerJobs(t *testing.T) {
	cfg := install.Config{Namespace: "demo", Application: "demo", ControllerImage: "controller:test", ResponderImage: "responder:test", RepositoryPVC: "repository", CredentialsSecret: "credentials", Model: "gpt-test", AcceptedCommit: strings.Repeat("a", 40), TimeoutSeconds: 60, NativeCommand: []string{"/usr/local/bin/sdo-operator"}, BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "registry-push"}
	operate, err := install.OperateJob(cfg, "healthy", "operation-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := install.Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{operate.Spec.Template.Spec.Containers[0].Args, plan.Job.Spec.Template.Spec.Containers[0].Args} {
		if !contains(args, "--metrics-url") {
			t.Fatalf("the default install must configure metrics: %v", args)
		}
	}
	cfg.MetricsURL = "http://prometheus.monitoring:9090"
	operate, err = install.OperateJob(cfg, "healthy", "operation-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	operateLaunch, err := operatorcli.ParseOperateLaunch(operate.Spec.Template.Spec.Containers[0].Args)
	if err != nil || operateLaunch.Install.MetricsURL != cfg.MetricsURL {
		t.Fatalf("operate child lost the metrics URL: %+v, %v", operateLaunch.Install, err)
	}
	plan, err = install.Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
<<<<<<< HEAD
	controllerLaunch, err := operatorcli.ParseControllerLaunch(plan.Job.Spec.Template.Spec.Containers[0].Args)
=======
	controllerLaunch, _, err := operatorcli.ParseControllerLaunch(append(plan.Job.Spec.Template.Spec.Containers[0].Args, "--operator-root", ".."))
>>>>>>> origin/vic-lsh/dev/sdo-improve
	if err != nil || controllerLaunch.Controller.GetMetricsUrl() != cfg.MetricsURL {
		t.Fatalf("controller child lost the metrics URL: %+v, %v", controllerLaunch.Controller, err)
	}
}

func TestMemoryUpstreamReachesTheControllerLaunchWithReadOnlyKey(t *testing.T) {
	cfg := install.Config{Namespace: "demo", Application: "demo", ControllerImage: "controller:test", ResponderImage: "responder:test", ValidatorImage: "validator:test", RepositoryPVC: "repository", CredentialsSecret: "credentials", Model: "gpt-test", AcceptedCommit: strings.Repeat("a", 40), TimeoutSeconds: 60, NativeCommand: []string{"/usr/local/bin/sdo-operator"}}
	plan, err := install.Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if contains(plan.Job.Spec.Template.Spec.Containers[0].Args, "--memory-upstream-url") {
		t.Fatal("the default install must keep accepted memory local")
	}
	cfg.MemoryUpstreamURL, cfg.MemoryUpstreamBranch, cfg.MemoryUpstreamSSHSecret = "git@github.com:example/demo-sdo.git", "live/20261028-synthetic", "memory-ssh"
	plan, err = install.Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	pod := plan.Job.Spec.Template.Spec
	launch, _, err := operatorcli.ParseControllerLaunch(append(pod.Containers[0].Args, "--operator-root", ".."))
	if err != nil {
		t.Fatal(err)
	}
	upstream := launch.Controller.GetMemoryUpstream()
	if upstream.GetUrl() != cfg.MemoryUpstreamURL || upstream.GetBranch() != cfg.MemoryUpstreamBranch || upstream.GetSshDir() != install.MemoryUpstreamSSHDir {
		t.Fatalf("controller lost the memory upstream: %+v", upstream)
	}
	mounted := false
	for _, mount := range pod.Containers[0].VolumeMounts {
		if mount.MountPath == install.MemoryUpstreamSSHDir {
			mounted = mount.ReadOnly
			for _, volume := range pod.Volumes {
				if volume.Name == mount.Name && (volume.Secret == nil || volume.Secret.SecretName != "memory-ssh") {
					mounted = false
				}
			}
		}
	}
	if !mounted {
		t.Fatal("ssh key Secret is not mounted read-only at the controller's ssh directory")
	}
	cfg.MemoryUpstreamBranch = ""
	if _, err := install.Resources(cfg); err == nil {
		t.Fatal("memory upstream without a branch was accepted")
	}
}
