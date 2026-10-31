package operatorcli

import (
	"context"
	"errors"
	"fmt"

	"path/filepath"
	"sdo.dev/operator/artifacts"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
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

	validation := broker.SubprocessValidator{}
	if config.GetHealthyBaseline() {
		validation.HealthyBaseline = filepath.Join(config.GetRepositoryMountPath(), ".sdo-baseline/healthy")
	}
	return broker.New(config.GetAppRoot(), config.GetBrokerWorktreeRoot(), validation.Validator())
}
