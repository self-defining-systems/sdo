package operatorcli

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sdo.dev/operator/contracts/settings"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/artifacts"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/install"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory/observation"
	"sdo.dev/operator/worker_registry"
)

// lifecycleRegistry loads the operator's agent registry: the mounted external
// manifests when configured, otherwise the built-in ones from builtInDirectory
// (empty selects the controller image's copy).
func lifecycleRegistry(config OperateConfig, builtInDirectory string) (*worker_registry.Registry, error) {
	if config.RegistrationDirectory != "" {
		return worker_registry.LoadDirectory(config.RegistrationDirectory)
	}
	return worker_registry.LoadBuiltIn(builtInDirectory, config.Install.ResponderImage)
}

// RunOperate performs bootstrap from the accepted PVC checkout. Every worker
// receives an independent broker proposal and no accepted repository mount.
func RunOperate(ctx context.Context, config OperateConfig) (lifecycle.Result, error) {
	if config.ApplicationRoot == "" || config.WorktreeRoot == "" || config.RepositoryMount == "" {
		return lifecycle.Result{}, errors.New("native operate launch requires accepted checkout, proposal root, and native installer")
	}
	if filepath.Clean(config.ApplicationRoot) != filepath.Join(filepath.Clean(config.RepositoryMount), "application") {
		return lifecycle.Result{}, errors.New("accepted checkout must be the application directory on the shared repository mount")
	}
	if config.BuildKitImage == "" || config.RegistryPushSecret == "" {
		return lifecycle.Result{}, errors.New("native source deployment requires BuildKit image and dedicated registry push Secret")
	}
	proposalSubPath, err := filepath.Rel(config.RepositoryMount, config.WorktreeRoot)
	if err != nil || proposalSubPath == "." || proposalSubPath == ".." || filepath.IsAbs(proposalSubPath) || len(proposalSubPath) >= 3 && proposalSubPath[:3] == "../" {
		return lifecycle.Result{}, errors.New("broker proposals must be inside the shared repository mount")
	}
	registry, err := lifecycleRegistry(config, "")
	if err != nil {
		return lifecycle.Result{}, fmt.Errorf("load lifecycle agents: %w", err)
	}
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return lifecycle.Result{}, fmt.Errorf("in-cluster Kubernetes credentials: %w", err)
	}
	restConfig.QPS, restConfig.Burst = 50, 100
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return lifecycle.Result{}, fmt.Errorf("Kubernetes client: %w", err)
	}
	control := config.Install.ControlNamespace
	if control == "" {
		control = config.Lifecycle.Namespace
	}
	validation := broker.SubprocessValidator{}
	if config.Install.HealthyBaseline {
		validation.HealthyBaseline = filepath.Join(config.RepositoryMount, ".sdo-baseline/healthy")
	}
	ceiling, policyErr := observation.CeilingFromEnvironment()
	if policyErr != nil {
		return lifecycle.Result{}, policyErr
	}

	repository, err := broker.New(config.ApplicationRoot, config.WorktreeRoot, selectMemoryValidator(config.Install.CodePath, validation.Validator()), broker.ObservationPolicy{Replay: config.Install.Replay, CodePath: memoryCodePath(config.Install.CodePath), Ceiling: ceiling, ApplicationNamespace: config.Lifecycle.Namespace})
	if err != nil {
		return lifecycle.Result{}, fmt.Errorf("open accepted repository broker: %w", err)
	}
	// The generic metrics detectors are installed only where a metrics source
	// is configured; the judge's own check and the broker must agree.
	config.Install.MetricsURL = config.Install.EffectiveMetricsURL()
	config.Lifecycle.MetricsSource = config.Install.MetricsSource()
	repository.MetricsDetectors = config.Install.MetricsURL != ""
	buildBackend := deployment.KubernetesBuildBackend{
		Client: client, Namespace: control, RepositoryPVC: config.Install.RepositoryPVC,
		ProposalRoot: config.WorktreeRoot, RepositoryPVCSubPath: proposalSubPath,
		BuildKitImage: config.BuildKitImage, RegistrySecret: config.RegistryPushSecret,
		AllowedRegistryHost: config.Install.RegistryHost,
		RegistryInsecure:    config.Install.RegistryInsecure,
		ServiceAccount:      "sdo-image-builder",
	}
	readiness, err := deployment.NewRegistry(deployment.RawRegistration("raw-manifests-v1", config.Lifecycle.Namespace, buildBackend))
	if err != nil {
		return lifecycle.Result{}, err
	}
	readiness.AcceptedRoot = config.ApplicationRoot
	agentEnvironment, err := worker_registry.AgentEnvironment(config.Install.AgentProvider, config.Install.Model, config.Install.AgentEffort)
	if err != nil {
		return lifecycle.Result{}, err
	}
	replay, err := settings.Replay(config.Install.Replay)
	if err != nil {
		return lifecycle.Result{}, err
	}
	agentEnvironment[settings.ReplayEnvironment] = replay
	agentEnvironment["CODEX_HOME"], agentEnvironment["CLAUDE_CONFIG_DIR"] = "/tmp/codex", "/tmp/claude"
	workers := worker_registry.KubernetesExecutor{
		ToolchainImage: config.Install.ControllerImage,
		Client:         client, Namespace: control, Registry: registry, ServiceAccount: "sdo-responder",
		RepositoryPVC: config.Install.RepositoryPVC, RepositoryMountPath: config.RepositoryMount,
		CredentialsSecret: config.Install.CredentialsSecret,
		Environment:       agentEnvironment,
	}
	workers.Environment = codePathWorkerEnvironment(config.Install.CodePath, workers.Environment)
	if config.Install.MetricsURL != "" {
		workers.Environment[memoryMetricsEnv] = "1"
	}
	installer := lifecycle.NativeInstallAdapter{
		Strategy: lifecycle.KubernetesInstallStrategy{Installer: install.Installer{Client: client}},
		Config:   config.Install, AcceptedRepository: config.ApplicationRoot, Prepared: true,
	}
	var healthProbe deployment.HealthProbe = deployment.AcceptedDetectorProbe{Replay: config.Install.Replay, MetricsURL: config.Install.MetricsURL}
	if runner, enabled := install.ShellRunnerConfig(config.Install); enabled {
		healthProbe = deployment.ShellDetectorProbe{Config: runner}
	}
	coordinator := lifecycle.Coordinator{
		Broker: repository, Bootstrap: artifacts.BootstrapStore{}, Registry: registry,
		Workers: workers, Readiness: readiness,
		Verifier: deployment.IndependentHealthVerifier{
			AcceptedRoot: config.ApplicationRoot, OperatorRoot: config.OperatorRoot, Readiness: readiness, Probe: healthProbe,
		}, Installer: installer,
	}
	return coordinator.Run(ctx, config.Lifecycle)
}
