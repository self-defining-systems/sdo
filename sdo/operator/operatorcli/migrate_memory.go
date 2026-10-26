package operatorcli

import (
	"context"
	"errors"
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/artifacts"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/observation"
	"sdo.dev/operator/toolchain"
)

// memoryMigrationOperationID names the accepted Upkeep migration for the
// current schema, so a restarted controller finds it instead of repeating it.
func memoryMigrationOperationID() string {
	return fmt.Sprintf("memory-v%d", layout.CurrentSchemaVersion)
}

// migrateAcceptedMemory has the Upkeep actor bring an already-bootstrapped
// repository to the current operational-memory schema through the broker. It is
// idempotent: an accepted migration is reused and a current repository is left
// untouched.
func migrateAcceptedMemory(ctx context.Context, repository lifecycle.ProposalBroker) error {
	coordinator := lifecycle.Coordinator{Broker: repository, Bootstrap: artifacts.BootstrapStore{}}
	_, err := coordinator.MigrateMemory(ctx, memoryMigrationOperationID())
	return err
}

// resolveAcceptedDiagnostics is the controller's first memory read. It migrates
// the accepted repository before projecting its diagnostics.
func resolveAcceptedDiagnostics(ctx context.Context, root string, repository lifecycle.ProposalBroker) (builder.DiagnosticsProjection, error) {
	if repository == nil {
		return builder.DiagnosticsProjection{}, errors.New("memory migration requires a broker")
	}
	if err := migrateAcceptedMemory(ctx, repository); err != nil {
		return builder.DiagnosticsProjection{}, err
	}
	projected, err := memory.AcceptedDiagnostics(ctx, root)
	if err != nil {
		return builder.DiagnosticsProjection{}, err
	}
	return builder.DiagnosticsProjection{Directory: projected.Directory, Fingerprint: projected.Fingerprint, AcceptedRevision: projected.AcceptedRevision}, nil
}

// openMigrationBroker builds the controller's broker from its typed launch
// config, with the same job validator the native incident broker uses.
func openMigrationBroker(config *contracts.ControllerConfig) (lifecycle.ProposalBroker, error) {
	if err := toolchain.ValidateImage(config.GetToolchainImage()); err != nil {
		return nil, err
	}

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("memory migration in-cluster credentials: %w", err)
	}
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("memory migration Kubernetes client: %w", err)
	}
	namespace := config.GetControlNamespace()
	if namespace == "" {
		namespace = config.GetNamespace()
	}
	job := broker.KubernetesValidationJobClient{
		ToolchainImage: config.GetToolchainImage(),
		Client:         client, Namespace: namespace, Image: config.GetValidatorImage(),
		RepositoryPVC: config.GetRepositoryPvc(), RepositoryMountPath: config.GetRepositoryMountPath(),
		RepositoryPVCSubPath: config.GetRepositoryPvcSubpath(), SchemaIdentity: config.GetSchemaIdentity(),
		HealthyBaseline: config.GetHealthyBaseline(),
	}
	ceiling, policyErr := observation.CeilingFromEnvironment()
	if policyErr != nil {
		return nil, policyErr
	}

	return broker.New(config.GetAppRoot(), config.GetBrokerWorktreeRoot(), selectMemoryValidator(config.GetCodePath(), broker.JobValidator(job)), broker.ObservationPolicy{CodePath: memoryCodePath(config.GetCodePath()), Ceiling: ceiling, ApplicationNamespace: config.GetNamespace()})
}
