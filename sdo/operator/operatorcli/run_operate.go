package operatorcli

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/deploymentpolicy"
	"sdo.dev/operator/renderenv"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/artifacts"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/install"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory/observation"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/telemetryapply"
	"sdo.dev/operator/telemetrycontext"
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

	repository, err := onboardingBroker(config, control, selectMemoryValidator(config.Install.CodePath, validation.Validator()), ceiling, telemetryRegistry())
	if err != nil {
		return lifecycle.Result{}, err
	}
	if config.Lifecycle.Telemetry, err = telemetrycontext.Build(telemetryRegistry(), config.Install.TelemetryEnvironment(), nil); err != nil {
		return lifecycle.Result{}, err
	}
	environment, err := renderenv.Discover(client.Discovery())
	if err != nil {
		return lifecycle.Result{}, fmt.Errorf("discover render environment: %w", err)
	}
	deployPolicy, err := deploymentpolicy.Parse(config.Install.DeploymentPolicy)
	if err != nil {
		return lifecycle.Result{}, err
	}
	readiness, err := operateIdentity(config).deployer(client, deployTarget{
		Namespace: config.Lifecycle.Namespace, ControlNamespace: control,
		AcceptedRoot: config.ApplicationRoot, WorkspaceRoot: config.WorktreeRoot,
		RepositoryMount: config.RepositoryMount, RepositoryPVC: config.Install.RepositoryPVC,
	}, environment, deployPolicy)
	if err != nil {
		return lifecycle.Result{}, err
	}
	workers, err := lifecycleWorkers(config, client, control, registry, repository)
	if err != nil {
		return lifecycle.Result{}, err
	}
	installer := lifecycle.NativeInstallAdapter{
		Strategy: lifecycle.KubernetesInstallStrategy{Installer: install.Installer{Client: client}, Renderers: renderers.Catalog()},
		Config:   config.Install, AcceptedRepository: config.ApplicationRoot, Prepared: true, Now: processio.Now,
	}
	telemetryEnvironment, removeTelemetryEnvironment, err := writeTelemetryEnvironment(config.Install)
	if err != nil {
		return lifecycle.Result{}, err
	}
	defer removeTelemetryEnvironment()
	var healthProbe deployment.HealthProbe = deployment.AcceptedDetectorProbe{Replay: config.Install.Replay, TelemetryEnvironment: telemetryEnvironment}
	if runner, enabled := install.ShellRunnerConfig(config.Install); enabled {
		healthProbe = deployment.ShellDetectorProbe{Config: runner}
	}
	coordinator := lifecycle.Coordinator{
		// The agent is offered exactly what the deployer can deploy.
		Renderers: readiness.Renderers, ImageSources: readiness.Images,
		Broker: repository, Bootstrap: artifacts.BootstrapStore{}, Registry: registry,
		Workers: workers, Readiness: readiness,
		// The designer's telemetry backends are applied under the same
		// policy the broker validated them against.
		TelemetryApply: telemetryapply.NewApplier(client, repository.TelemetryPolicy(), config.Install.BackendRecipes),
		Verifier: deployment.IndependentHealthVerifier{
			AcceptedRoot: config.ApplicationRoot, OperatorRoot: config.OperatorRoot, Readiness: &readiness, Probe: healthProbe,
		}, Installer: installer,
	}
	return coordinator.Run(ctx, config.Lifecycle)
}

// operateIdentity is the operate Job's trusted build and apply identity; the
// installer passes the same one to the controller parent for advances.
func operateIdentity(config OperateConfig) deployIdentity {
	return deployIdentity{
		BuildKitImage: config.BuildKitImage, RegistryPushSecret: config.RegistryPushSecret,
		RegistryHost: config.Install.RegistryHost, RegistryInsecure: config.Install.RegistryInsecure,
		BuildTimeout:   time.Duration(config.Install.BuildTimeoutSeconds) * time.Second,
		BuildBudget:    time.Duration(config.Install.BuildBudgetSeconds) * time.Second,
		RolloutTimeout: time.Duration(config.Install.RolloutTimeoutSeconds) * time.Second,
	}
}

// lifecycleWorkers is the operate Job's agent transport: every lifecycle agent
// runs as a Job in the control namespace with the install's model settings,
// and the broker's policy environment, so the health judge's own `memory
// check` and the broker judge one proposal by the same rules.
func lifecycleWorkers(config OperateConfig, client kubernetes.Interface, control string, registry *worker_registry.Registry, repository *broker.Broker) (worker_registry.KubernetesExecutor, error) {
	agentEnvironment, err := worker_registry.AgentEnvironment(config.Install.AgentProvider, config.Install.Model, config.Install.AgentEffort, config.Install.LifecycleTimeout)
	if err != nil {
		return worker_registry.KubernetesExecutor{}, err
	}
	replay, err := settings.Replay(config.Install.Replay)
	if err != nil {
		return worker_registry.KubernetesExecutor{}, err
	}
	agentEnvironment[settings.ReplayEnvironment] = replay
	workers := worker_registry.KubernetesExecutor{
		ToolchainImage: config.Install.ControllerImage,
		Client:         client, Namespace: control, Registry: registry, ServiceAccount: "sdo-responder",
		RepositoryPVC: config.Install.RepositoryPVC, RepositoryMountPath: config.RepositoryMount,
		CredentialsSecret: config.Install.CredentialsSecret,
		Environment:       agentEnvironment,
	}
	workers.Environment = codePathWorkerEnvironment(config.Install.CodePath, workers.Environment)
	policy, err := repository.PolicyEnvironment()
	if err != nil {
		return worker_registry.KubernetesExecutor{}, err
	}
	for name, value := range policy {
		workers.Environment[name] = value
	}
	return workers, nil
}
