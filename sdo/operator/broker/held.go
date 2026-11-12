package broker

import (
	"context"
	"fmt"
	"log"
)

// HeldBranchPrefix names the remote side branches that keep a source change SDO
// could not rebase onto the branch head: sdo-held/<incident>.
const HeldBranchPrefix = "sdo-held/"

// HoldSource keeps the source a responder committed when it conflicts with
// the branch head, so a person can merge it. The agent's own commits (on the
// base they were made on) are kept under a local ref and, when the accepted
// repository has a remote, pushed to the side branch sdo-held/<incident>. It
// never touches the source branch and nothing is deployed. The returned
// branch is "" when there is no remote to push to or the push failed (the
// local ref still holds the commits).
func (b *Broker) HoldSource(ctx context.Context, incidentID string) (branch string, err error) {
	if err := checkIncidentID(incidentID); err != nil {
		return "", err
	}
	proposal, err := b.proposal(incidentID, ProposalPhase)
	if err != nil {
		return "", err
	}
	head, err := gitText(ctx, proposal.Path, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	ref := "refs/sdo/held/" + incidentID
	err = b.locked(func() error {
		if _, err := runGit(ctx, b.target, "fetch", "--no-tags", "--quiet", proposal.Path, head+":"+ref); err != nil {
			return fmt.Errorf("keep the conflicting source of %s: %w", incidentID, err)
		}
		if b.upstream == nil {
			return nil
		}
		if err := b.pushRef(ctx, b.target, ref, HeldBranchPrefix+incidentID); err != nil {
			log.Printf("held source of %s kept locally; the side branch push failed: %v", incidentID, err)
			return nil
		}
		branch = HeldBranchPrefix + incidentID
		return nil
	})
	return branch, err
}
