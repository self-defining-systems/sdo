package broker

import (
	"context"
	"errors"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
)

// CheckBase runs the publication validator over the untouched base commit of a
// proposal, in a private clone, as the actor and phase that will publish it.
// What it returns fails before any agent edit, so the agent cannot be blamed
// and cannot fix it (a stale detector package, an invalid accepted file). The
// empty-diff diagnostic is the one expected finding of an unedited tree and is
// excluded: it is what the agent's edit is for.
func (b *Broker) CheckBase(ctx context.Context, incidentID string, actor Actor, phase Phase) ([]*contracts.MemoryDiagnostic, error) {
	if b.validator == nil && b.codePath != "shell" {
		return nil, errors.New("base validation is not configured")
	}
	var diagnostics []validate.Diagnostic
	err := b.locked(func() error {
		proposal, err := b.proposal(incidentID, phase)
		if err != nil {
			return err
		}
		stage, cleanup, err := b.privateClone(ctx)
		if err != nil {
			return err
		}
		defer cleanup()
		if _, err := runGit(ctx, stage, "checkout", "--quiet", "--detach", proposal.BaseCommit); err != nil {
			return err
		}
		publication := &validate.PublicationContext{BaseCommit: proposal.BaseCommit, IncidentID: incidentID}
		all, err := b.checkCandidate(ctx, stage, proposal.BaseCommit, actor, phase, nil, b.validator, publication, nil, nil)
		if err != nil {
			return err
		}
		for _, d := range all {
			if d.Rule != validate.RuleEmptyDiff {
				diagnostics = append(diagnostics, d)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return validate.ProtoDiagnostics(diagnostics), nil
}
