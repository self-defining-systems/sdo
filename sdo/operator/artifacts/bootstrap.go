// Package artifacts adapts lifecycle's logical bootstrap contract to memory.
package artifacts

import (
	"context"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

type BootstrapStore struct{}

func (BootstrapStore) ReadArchitecture(checkout string) (string, error) {
	return memory.ReadArchitecture(checkout)
}

func (BootstrapStore) WriteArtifacts(checkout string, proposal *contracts.OperationalArtifactProposal, role memory.ArtifactRole) error {
	if err := memory.RequireCleanProposalCheckout(context.Background(), checkout); err != nil {
		return err
	}
	return memory.MaterializeArtifacts(checkout, proposal, role)
}

func (BootstrapStore) WriteGoal(checkout, application, objective string) error {
	return memory.WriteGoal(checkout, application, objective)
}

func (BootstrapStore) WriteSchema(checkout string) error {
	return memory.WriteSchema(checkout)
}

func (BootstrapStore) WriteArchitecture(checkout, application, acceptedSourceCommit, fingerprint, summary string) error {
	return memory.WriteArchitecture(checkout, application, acceptedSourceCommit, fingerprint, summary)
}
