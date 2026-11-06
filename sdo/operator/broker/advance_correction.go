package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/planfile"
)

// advanceCorrectionTrailer marks an accepted advance whose deployment files,
// patches or plan the deployment agent corrected before it was deployed.
const advanceCorrectionTrailer = "SDO-Advance-Correction"

// CorrectAdvance commits the deployment agent's correction of advance id. The
// agent edits the advance checkout (accepted HEAD merged with upstream, HEAD:
// the candidate) in files under .sdo/deployment/; Go writes plan as the
// canonical .sdo/deployment/plan.yaml. The correction is validated as the
// deployer's advance route (the shared grant table: deployment files and
// patches it owns, never app files and never other memory) and the effective
// tree (base plus patches) must apply. A rejection is returned as the text the
// agent's next request carries, and the checkout is reset to the candidate. On
// success the correction is a commit on top of the candidate and the new HEAD is
// returned. PublishAdvance replays it onto its own recomputed merge.
func (b *Broker) CorrectAdvance(ctx context.Context, id string, plan *contracts.DeploymentPlan) (candidate, rejection string, err error) {
	if err := checkIncidentID(id); err != nil {
		return "", "", err
	}
	if plan.GetSource() == nil {
		return "", "the corrected plan names no deployment source", nil
	}
	err = b.locked(func() error {
		proposal, err := b.proposal(id, AdvancePhase)
		if err != nil {
			return fmt.Errorf("advance proposal %s: %w", id, err)
		}
		head, err := gitText(ctx, proposal.Path, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		reset := func() error {
			if _, err := runGit(ctx, proposal.Path, "read-tree", "--reset", "-u", "HEAD"); err != nil {
				return err
			}
			_, err := runGit(ctx, proposal.Path, "clean", "-fdq")
			return err
		}
		reject := func(text string) error {
			rejection = text
			return reset()
		}
		if err := planfile.Write(proposal.Path, plan); err != nil {
			return reject("the corrected plan cannot be written: " + err.Error())
		}
		paths, err := validate.ChangedPaths(ctx, proposal.Path, head)
		if err != nil {
			return err
		}
		changed := partitionPaths(paths)
		if len(changed.Scratch) > 0 {
			return reject(fmt.Sprintf("app files are never edited in an advance, only deployment files and patches under %s: %s", layout.DeploymentDir, strings.Join(changed.Scratch, ", ")))
		}
		if len(changed.SDO) == 0 {
			return reject("the correction changed nothing: edit the deployment files, patches or plan so the hold's checks pass")
		}
		tree, err := advanceTreeDigest(ctx, proposal.Path)
		if err != nil {
			return err
		}
		// The checkout's tree is the one the deployer will deploy and verify, so
		// the advance rules' verified-tree binding is its digest here; PublishAdvance
		// binds the tree that was actually deployed.
		publication := &validate.PublicationContext{BaseCommit: head, IncidentID: id, VerifiedSourceTree: tree}
		// The compile transport is the publish's job; the ownership, deployment
		// and plan rules are what an agent can get wrong here.
		diagnostics, err := b.checkCandidate(ctx, proposal.Path, head, Deployer, AdvancePhase, changed.SDO, func(context.Context, string, []string) error { return nil }, publication, nil, nil)
		if err != nil {
			return err
		}
		if len(diagnostics) > 0 {
			return reject(validate.Err(diagnostics).Error())
		}
		if err := checkEffective(ctx, proposal.Path, tree); err != nil {
			var patchErr *patchset.PatchError
			if errors.As(err, &patchErr) {
				return reject("the patch series does not apply to the corrected tree: " + err.Error())
			}
			return err
		}
		if _, err := runGit(ctx, proposal.Path, "add", "--all", "--", "."); err != nil {
			return err
		}
		message := fmt.Sprintf("sdo(%s): deployer advance correction\n\nSDO-Actor: %s\nSDO-Phase: %s", id, Deployer, AdvancePhase)
		if _, err := runGitEnv(ctx, proposal.Path, brokerGitEnv(), "commit", "--quiet", "--no-verify", "-m", message); err != nil {
			return err
		}
		candidate, err = gitText(ctx, proposal.Path, "rev-parse", "HEAD")
		return err
	})
	return candidate, rejection, err
}

// advanceCorrectionCommits are the corrections committed on an advance
// checkout: merge is the merge candidate (the first commit above the accepted
// base), tip the last correction, paths what they changed relative to merge.
type advanceCorrectionCommits struct {
	merge, tip string
	paths      []string
}

// advanceCorrection reads the corrections committed on the advance checkout,
// nil when there are none. Only paths under .sdo/ may differ between the merge
// candidate and the corrected tip.
func (b *Broker) advanceCorrection(ctx context.Context, proposal Proposal) (*advanceCorrectionCommits, error) {
	out, err := gitText(ctx, proposal.Path, "rev-list", "--first-parent", proposal.BaseCommit+"..HEAD")
	if err != nil {
		return nil, err
	}
	commits := strings.Fields(out)
	if len(commits) <= 1 {
		return nil, nil
	}
	found := &advanceCorrectionCommits{tip: commits[0], merge: commits[len(commits)-1]}
	changed, err := runGit(ctx, proposal.Path, "diff", "--no-renames", "--name-only", "-z", found.merge, found.tip, "--")
	if err != nil {
		return nil, err
	}
	for _, path := range strings.Split(string(changed), "\x00") {
		if path == "" {
			continue
		}
		if !layout.IsMemoryPath(path) {
			return nil, fmt.Errorf("advance correction changes the app file %s", path)
		}
		found.paths = append(found.paths, path)
	}
	return found, nil
}

// replayCorrection applies the corrections onto stage, which holds the
// broker's own recomputation of the merge, and makes the merge candidate
// available as the base the correction is judged against.
func (b *Broker) replayCorrection(ctx context.Context, stage string, proposal Proposal, correction *advanceCorrectionCommits) error {
	if _, err := runGit(ctx, stage, "-c", "uploadpack.allowAnySHA1InWant=true", "fetch", "--quiet", "--no-tags", proposal.Path, correction.tip); err != nil {
		return fmt.Errorf("fetch advance correction %s: %w", correction.tip, err)
	}
	for _, path := range correction.paths {
		if _, err := runGit(ctx, stage, "cat-file", "-e", correction.tip+":"+path); err != nil {
			if _, rmErr := runGit(ctx, stage, "rm", "-f", "-q", "--", path); rmErr != nil {
				return rmErr
			}
			continue
		}
		if _, err := runGit(ctx, stage, "checkout", correction.tip, "--", path); err != nil {
			return err
		}
	}
	return nil
}
