package operatorcli

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/artifacts"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/worker_registry"
)

// Refresh runs before the builder projects diagnostics. Failure prevents launch
// of a controller compiled from stale requirements, including seeded startup.
func refreshControllerHealth(ctx context.Context, config *contracts.ControllerConfig, repository lifecycle.ProposalBroker) error {
	revision, err := memory.AcceptedHealthRevision(ctx, config.GetAppRoot())
	if err != nil {
		return err
	}
	if !revision.NeedsRefresh {
		return nil
	}
	var registry *worker_registry.Registry
	if config.GetAgentRegistrationDirectory() == "" {
		registry, err = worker_registry.LoadBuiltIn("", config.GetResponderImage())
	} else {
		registry, err = worker_registry.LoadDirectory(config.GetAgentRegistrationDirectory())
	}
	if err != nil {
		return err
	}
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("health refresh credentials: %w", err)
	}
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return err
	}
	environment, err := worker_registry.AgentEnvironment(config.GetResponderBackend(), config.GetResponderModel())
	if err != nil {
		return err
	}
	environment["CODEX_HOME"], environment["CLAUDE_CONFIG_DIR"] = "/tmp/codex", "/tmp/claude"
	namespace := config.GetControlNamespace()
	if namespace == "" {
		namespace = config.GetNamespace()
	}
	coordinator := lifecycle.Coordinator{Broker: repository, Bootstrap: artifacts.BootstrapStore{}, Registry: registry, Workers: worker_registry.KubernetesExecutor{
		Client: client, Namespace: namespace, Registry: registry, ServiceAccount: "sdo-responder", RepositoryPVC: config.GetRepositoryPvc(), RepositoryMountPath: config.GetRepositoryMountPath(), RepositoryPVCSubPath: config.GetRepositoryPvcSubpath(), CredentialsSecret: config.GetResponderCredentialsSecret(), HealthyBaseline: config.GetHealthyBaseline(), Environment: environment,
	}}
	if _, err := coordinator.RefreshHealth(ctx, lifecycle.Config{Application: config.GetApplication(), Namespace: config.GetNamespace()}, revision); err != nil {
		return err
	}
	accepted, err := memory.AcceptedHealthRevision(ctx, config.GetAppRoot())
	if err != nil {
		return err
	}
	if accepted.NeedsRefresh {
		return fmt.Errorf("health unverified: source, objective or detectors advanced during refresh")
	}
	return nil
}
