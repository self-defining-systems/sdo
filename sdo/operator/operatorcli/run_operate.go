package operatorcli

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/artifacts"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/install"
	"sdo.dev/operator/lifecycle"
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
	validation := broker.KubernetesValidationJobClient{
		Client: client, Namespace: control, Image: config.Install.ValidatorImage,
		RepositoryPVC: config.Install.RepositoryPVC, RepositoryMountPath: config.RepositoryMount,
		SchemaIdentity: sdkSchemaIdentity, HealthyBaseline: config.Install.HealthyBaseline,
	}
	repository, err := broker.New(config.ApplicationRoot, config.WorktreeRoot, broker.JobValidator(validation))
	if err != nil {
		return lifecycle.Result{}, fmt.Errorf("open accepted repository broker: %w", err)
	}
	// The generic metrics detectors are installed only where a metrics source
	// is configured; the judge's own check and the broker must agree.
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
	workers := worker_registry.KubernetesExecutor{
		Client: client, Namespace: control, Registry: registry, ServiceAccount: "sdo-responder",
		RepositoryPVC: config.Install.RepositoryPVC, RepositoryMountPath: config.RepositoryMount,
		CredentialsSecret: config.Install.CredentialsSecret,
		Environment: map[string]string{
			"SDO_AGENT_PROVIDER": config.Install.AgentProvider, "SDO_LIFECYCLE_MODEL": config.Install.Model,
			"SDO_DEPLOYMENT_MODEL": config.Install.Model, "CODEX_HOME": "/tmp/codex", "CLAUDE_CONFIG_DIR": "/tmp/claude",
		},
	}
	if config.Install.MetricsURL != "" {
		workers.Environment[memoryMetricsEnv] = "1"
	}
	installer := lifecycle.NativeInstallAdapter{
		Strategy: lifecycle.KubernetesInstallStrategy{Installer: install.Installer{Client: client}},
		Config:   config.Install, AcceptedRepository: config.ApplicationRoot, Prepared: true,
	}
	coordinator := lifecycle.Coordinator{
		Broker: repository, Bootstrap: artifacts.BootstrapStore{}, Registry: registry,
		Workers: workers, Readiness: readiness,
		Verifier: deployment.IndependentHealthVerifier{
			AcceptedRoot: config.ApplicationRoot, OperatorRoot: config.OperatorRoot, Readiness: readiness,
		}, Installer: installer,
	}
	return coordinator.Run(ctx, config.Lifecycle)
}
