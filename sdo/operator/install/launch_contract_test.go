package install_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/install"
	"sdo.dev/operator/operatorcli"
)

func optionalLaunchConfig() install.Config {
	return install.Config{
		Namespace: "demo", ControlNamespace: "demo-sdo", Application: "demo",
		ControllerImage: "controller:test", ResponderImage: "responder:test", ValidatorImage: "validator:test",
		RepositoryPVC: "repository", CredentialsSecret: "credentials", Model: "test-model", AgentProvider: "claude",
		AcceptedCommit: strings.Repeat("a", 40), NativeCommand: []string{"/usr/local/bin/sdo-operator"},
		TimeoutSeconds: 180, VerificationTimeoutSeconds: 45, RepairPolicy: "recorded-actions",
		ReflectionSession: "fresh", ReflectionGuidance: "generalize-spec", ForceReflection: true,
		MaxResponseLoops: 7,
		ProgramFastPath:  true, ProgramTrustThreshold: 4,
		HealthyBaseline: true, LateFindings: "pull", ControllerDurationSeconds: 90, ExitAfterClosure: true, WaitForCompletion: true,
		ResponderEnv:          []string{"EXAMPLE_MODE=contract", "EXAMPLE_URL=http://example:8000"},
		RegistrationConfigMap: "sdo-agents", RegistrationDirectory: "/sdo/custom-registrations",
		BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("b", 64), RegistryPushSecret: "registry-push",
		RegistryHost: "registry.demo-sdo.svc.cluster.local:5000", RegistryInsecure: true,
	}
}

func TestInstallerOperateOptionalArgumentsRoundTripToController(t *testing.T) {
	cfg := optionalLaunchConfig()
	job, err := install.OperateJob(cfg, "requests succeed", "operation-contract", 2)
	if err != nil {
		t.Fatal(err)
	}
	launch, err := operatorcli.ParseOperateLaunch(job.Spec.Template.Spec.Containers[0].Args)
	if err != nil {
		t.Fatal(err)
	}
	wantInstall := cfg
	// These belong to host preparation or the operate process, rather than the
	// controller installation returned by the operate parser.
	wantInstall.AcceptedCommit = ""
	wantInstall.BuildKitImage = ""
	wantInstall.RegistryPushSecret = ""
	wantInstall.ReuseExisting = true
	if !reflect.DeepEqual(launch.Install, wantInstall) {
		t.Fatalf("operate install config did not round-trip:\n got: %+v\nwant: %+v", launch.Install, wantInstall)
	}
	if launch.Lifecycle.OperationID != "operation-contract" || launch.Lifecycle.HealthObjective != "requests succeed" || launch.Lifecycle.MaxAttempts != 2 || launch.Lifecycle.Application != cfg.Application || launch.Lifecycle.Namespace != cfg.Namespace || launch.Lifecycle.RegistryHost != cfg.RegistryHost {
		t.Fatalf("operate lifecycle config did not round-trip: %+v", launch.Lifecycle)
	}
	if launch.ApplicationRoot != "/workspace/application" || launch.RepositoryMount != "/workspace" || launch.WorktreeRoot != "/workspace/worktrees" || launch.BuildKitImage != cfg.BuildKitImage || launch.RegistryPushSecret != cfg.RegistryPushSecret || launch.RegistrationDirectory != cfg.RegistrationDirectory {
		t.Fatalf("operate process config did not round-trip: %+v", launch)
	}
	// The lifecycle supplies the accepted revision before installing. Exercise
	// that second real builder/parser boundary so a parsed budget cannot be lost.
	launch.Install.AcceptedCommit = cfg.AcceptedCommit
	plan, err := install.Resources(launch.Install)
	if err != nil {
		t.Fatal(err)
	}
	controller, supervise, err := operatorcli.ParseControllerLaunch(plan.Job.Spec.Template.Spec.Containers[0].Args)
	if err != nil {
		t.Fatal(err)
	}
	if supervise {
		t.Fatal("bounded controller must not supervise")
	}
	assertControllerLaunchRoundTrip(t, cfg, controller.Controller)
}

func TestInstallerControllerOptionalArgumentsRoundTrip(t *testing.T) {
	cfg := optionalLaunchConfig()
	cfg.NativeArgs = []string{"--repository-pvc-subpath", "accepted", "--source-commit", cfg.AcceptedCommit}
	cfg.ExtraControllerArgs = []string{"--deployed-manifest-path", "kubernetes", "--deployed-manifest-path", "services", "--deployed-commit", strings.Repeat("c", 40)}
	plan, err := install.Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	launch, supervise, err := operatorcli.ParseControllerLaunch(plan.Job.Spec.Template.Spec.Containers[0].Args)
	if err != nil {
		t.Fatal(err)
	}
	if supervise {
		t.Fatal("bounded controller must not supervise")
	}
	assertControllerLaunchRoundTrip(t, cfg, launch.Controller)
	if launch.Controller.GetRepositoryPvcSubpath() != "accepted" || launch.Controller.GetSourceCommit() != cfg.AcceptedCommit || launch.Controller.GetDeployedCommit() != strings.Repeat("c", 40) || !reflect.DeepEqual(launch.Controller.GetDeployedManifestPaths(), []string{"kubernetes", "services"}) {
		t.Fatalf("controller extension arguments did not round-trip: %+v", launch.Controller)
	}
}

func assertControllerLaunchRoundTrip(t *testing.T, cfg install.Config, launch *contracts.ControllerConfig) {
	t.Helper()
	// Late findings reach the responder only as this worker setting.
	wantResponderEnv := append([]string{}, cfg.ResponderEnv...)
	if cfg.LateFindings == "pull" {
		wantResponderEnv = append(wantResponderEnv, "SDO_LATE_FINDINGS=pull")
	}
	for name, pair := range map[string][2]any{
		"namespace":               {launch.GetNamespace(), cfg.Namespace},
		"control namespace":       {launch.GetControlNamespace(), cfg.ControlNamespace},
		"application":             {launch.GetApplication(), cfg.Application},
		"app root":                {launch.GetAppRoot(), "/workspace/application"},
		"responder image":         {launch.GetResponderImage(), cfg.ResponderImage},
		"prober image":            {launch.GetProberImage(), cfg.ControllerImage},
		"validator image":         {launch.GetValidatorImage(), cfg.ValidatorImage},
		"repository PVC":          {launch.GetRepositoryPvc(), cfg.RepositoryPVC},
		"repository mount":        {launch.GetRepositoryMountPath(), "/workspace"},
		"worktree root":           {launch.GetBrokerWorktreeRoot(), "/workspace/worktrees"},
		"credentials":             {launch.GetResponderCredentialsSecret(), cfg.CredentialsSecret},
		"model":                   {launch.GetResponderModel(), cfg.Model},
		"provider":                {launch.GetResponderBackend(), cfg.AgentProvider},
		"repair policy":           {launch.GetRepairPolicy(), cfg.RepairPolicy},
		"reflection session":      {launch.GetReflectionSession(), cfg.ReflectionSession},
		"reflection guidance":     {launch.GetReflectionGuidance(), cfg.ReflectionGuidance},
		"force reflection":        {launch.GetForceReflection(), cfg.ForceReflection},
		"response loops":          {launch.GetMaxResponseLoops(), int32(cfg.MaxResponseLoops)},
		"program fast path":       {launch.GetProgramFastPath(), cfg.ProgramFastPath},
		"program trust threshold": {launch.GetProgramTrustThreshold(), int32(cfg.ProgramTrustThreshold)},
		"healthy baseline":        {launch.GetHealthyBaseline(), cfg.HealthyBaseline},
		"duration":                {launch.GetDuration().AsDuration(), time.Duration(cfg.ControllerDurationSeconds) * time.Second},
		"exit after closure":      {launch.GetExitAfterClosure(), cfg.ExitAfterClosure},
		"response timeout":        {launch.GetResponseTimeout().AsDuration(), time.Duration(cfg.TimeoutSeconds) * time.Second},
		"verification timeout":    {launch.GetVerificationTimeout().AsDuration(), time.Duration(cfg.VerificationTimeoutSeconds) * time.Second},
		"responder environment":   {launch.GetResponderEnv(), wantResponderEnv},
		"registration directory":  {launch.GetAgentRegistrationDirectory(), cfg.RegistrationDirectory},
		"broker mode":             {launch.GetBrokerMode(), "native"},
		"worker mount mode":       {launch.GetWorkerMountMode(), "proposal-only"},
	} {
		if !reflect.DeepEqual(pair[0], pair[1]) {
			t.Errorf("%s: got %v, want %v", name, pair[0], pair[1])
		}
	}
}

func TestInstallerOperateLateFindingsOffAddsNoResponderSetting(t *testing.T) {
	for _, mode := range []string{"", "off"} {
		cfg := optionalLaunchConfig()
		cfg.LateFindings = mode
		job, err := install.OperateJob(cfg, "requests succeed", "operation-late-off", 1)
		if err != nil {
			t.Fatal(err)
		}
		launch, err := operatorcli.ParseOperateLaunch(job.Spec.Template.Spec.Containers[0].Args)
		if err != nil {
			t.Fatal(err)
		}
		launch.Install.AcceptedCommit = cfg.AcceptedCommit
		plan, err := install.Resources(launch.Install)
		if err != nil {
			t.Fatal(err)
		}
		controller, _, err := operatorcli.ParseControllerLaunch(plan.Job.Spec.Template.Spec.Containers[0].Args)
		if err != nil {
			t.Fatal(err)
		}
		cfg.LateFindings = "off"
		assertControllerLaunchRoundTrip(t, cfg, controller.Controller)
	}
}

func TestPermissiveResponderOperateLaunchRoundTrip(t *testing.T) {
	cfg := optionalLaunchConfig()
	cfg.PermissiveResponder = true
	cfg.ResponderNamespaces = []string{"policy-system"}
	job, err := install.OperateJob(cfg, "requests succeed", "permissive-contract", 2)
	if err != nil {
		t.Fatal(err)
	}
	launch, err := operatorcli.ParseOperateLaunch(job.Spec.Template.Spec.Containers[0].Args)
	if err != nil {
		t.Fatal(err)
	}
	if !launch.Install.PermissiveResponder || !reflect.DeepEqual(launch.Install.ResponderNamespaces, cfg.ResponderNamespaces) {
		t.Fatalf("permissions lost in operate launch: %+v", launch.Install)
	}
}
