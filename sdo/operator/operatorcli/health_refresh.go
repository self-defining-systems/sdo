package operatorcli

import (
	"context"
	"fmt"
	"time"

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
	return withHealthRefreshDeadline(ctx, config.GetDuration().AsDuration(), func(ctx context.Context) error {
		return refreshControllerHealthOnce(ctx, config, repository)
	})
}

// All refresh work, including an unschedulable Job, consumes the install budget.
// Unbounded installations still have a finite judge budget. A timed-out Job and
// its proposal are retained so a restart can attach to the identical task.
const defaultHealthRefreshTimeout = 15 * time.Minute

func withHealthRefreshDeadline(ctx context.Context, budget time.Duration, refresh func(context.Context) error) error {
	if budget <= 0 || budget > defaultHealthRefreshTimeout {
		budget = defaultHealthRefreshTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	if err := refresh(ctx); err != nil {
		return fmt.Errorf("health unverified: refresh judge failed within %s: %w", budget, err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("health unverified: refresh judge exceeded %s: %w", budget, err)
	}
	return nil
}

func refreshControllerHealthOnce(ctx context.Context, config *contracts.ControllerConfig, repository lifecycle.ProposalBroker) error {
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
		ToolchainImage: config.GetToolchainImage(),
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
