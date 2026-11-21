package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"sdo.dev/operator/rendered"
)

// A branch head that SDO did not write (an engineer's push, or the benchmark
// replayer's) is deployed and verified like any new source. SDO adds no trailer
// to another person's commit, so the deployment is recorded in the evidence
// store keyed by the commit, and newestDeployment reads it from there.

func headDeploymentID(commit string) string { return "head-" + commit }

// HeadChange is a branch head whose effective source needs a deployment.
type HeadChange struct {
	Commit string
	// EffectiveTree and PatchesDigest identify the source a deployment (or a
	// hold) is about; a hold is superseded only when they change.
	EffectiveTree string
	PatchesDigest string
}

// HeadToDeploy reports the accepted head when its effective source differs
// from the newest recorded deployment, after syncing the memory remote. It is
// false for an install with no recorded deployment (a seeded one), whose
// source nothing SDO owns can deploy. A memory-only head changes no effective
// tree and needs no deployment.
func (b *Broker) HeadToDeploy(ctx context.Context) (change HeadChange, needed bool, err error) {
	err = b.locked(func() error {
		if err := b.syncUpstream(ctx); err != nil {
			return err
		}
		recorded, found, err := b.acceptedEvidenceLocked(ctx)
		if err != nil {
			return err
		}
		if !found {
			// A seeded install (install-existing) adopted what runs and
			// recorded no deployment. The source it adopted is SDO's own newest
			// commit; an engineer's push that changes the effective source
			// after it is a head to deploy like any other.
			if recorded, found, err = b.seedBaselineLocked(ctx); err != nil || !found {
				return err
			}
		}
		head, err := gitText(ctx, b.target, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		if recorded.AcceptedCommit == head {
			return nil
		}
		tree, err := gitText(ctx, b.target, "rev-parse", head+"^{tree}")
		if err != nil {
			return err
		}
		effective, err := rendered.Effective(ctx, rendered.ObjectStore{Dir: b.target}, rendered.BaseTree(tree))
		if err != nil {
			return fmt.Errorf("compute the effective tree of head %s: %w", head, err)
		}
		defer effective.Close()
		if effective.ID == recorded.EffectiveTree && effective.PatchesDigest == recorded.PatchesDigest {
			return nil
		}
		change, needed = HeadChange{Commit: head, EffectiveTree: effective.ID, PatchesDigest: effective.PatchesDigest}, true
		return nil
	})
	return change, needed, err
}

// HeadCheckout is a clean checkout of an accepted commit, for the deployer and
// the independent verifier. release removes it.
func (b *Broker) HeadCheckout(ctx context.Context, commit string) (path string, release func(), err error) {
	if !validGitID(commit) {
		return "", nil, errors.New("head commit must be a full commit ID")
	}
	err = b.locked(func() error {
		var cleanup func()
		var stage string
		if stage, cleanup, err = b.privateClone(ctx); err != nil {
			return err
		}
		if _, err = runGit(ctx, stage, "checkout", "--quiet", "--detach", commit); err != nil {
			cleanup()
			return err
		}
		path, release = stage, cleanup
		return nil
	})
	return path, release, err
}

// RecordHeadDeployment records that the trusted deployer applied and the
// independent verifier cleared commit, an accepted commit the broker did not
// write. The evidence must be of exactly that commit's tree and effective
// tree; it becomes the newest deployment, so image pins and drift follow it.
func (b *Broker) RecordHeadDeployment(ctx context.Context, commit string, evidence SourceEvidence) error {
	if err := validateSourceEvidence(evidence); err != nil {
		return err
	}
	return b.locked(func() error {
		head, err := gitText(ctx, b.target, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		if commit != head {
			if _, err := runGit(ctx, b.target, "merge-base", "--is-ancestor", commit, head); err != nil {
				return fmt.Errorf("deployed commit %s is not in accepted history", commit)
			}
		}
		tree, err := gitText(ctx, b.target, "rev-parse", commit+"^{tree}")
		if err != nil {
			return err
		}
		if evidence.BaseTree != tree {
			return fmt.Errorf("%w: deployment evidence is of tree %s, the commit has tree %s", ErrCandidateUnbound, evidence.BaseTree, tree)
		}
		if err := requireEffectiveEvidence(ctx, b.target, tree, evidence); err != nil {
			return fmt.Errorf("%w: %w", ErrCandidateUnbound, err)
		}
		if err := requirePlanEvidence(ctx, b.target, evidence, b.PlanCatalogs); err != nil {
			return fmt.Errorf("%w: %w", ErrCandidateUnbound, err)
		}
		evidence.AcceptedCommit = commit
		return b.writeSourceEvidence(headDeploymentID(commit), evidence)
	})
}

// headDeployed reports whether commit, a commit the broker did not write, is
// a recorded head deployment.
func (b *Broker) headDeployed(ctx context.Context, commit string) (bool, error) {
	return b.deployedBy(ctx, headDeploymentID(commit), commit)
}

// seedBaselineLocked is the reference of an install with no recorded deployment:
// the effective source of the newest commit SDO wrote or installed (one that
// carries an SDO-Actor trailer; someone else's commit never does). found is
// false when the history holds none.
func (b *Broker) seedBaselineLocked(ctx context.Context) (SourceEvidence, bool, error) {
	out, err := runGit(ctx, b.target, "log", "--first-parent", "--format=%H%x1f%(trailers:key=SDO-Actor,valueonly)%x1e", "HEAD")
	if err != nil {
		return SourceEvidence{}, false, err
	}
	for _, record := range strings.Split(string(out), "\x1e") {
		fields := strings.Split(strings.TrimSpace(record), "\x1f")
		if len(fields) != 2 || strings.TrimSpace(fields[1]) == "" || !validGitID(strings.TrimSpace(fields[0])) {
			continue
		}
		commit := strings.TrimSpace(fields[0])
		tree, err := gitText(ctx, b.target, "rev-parse", commit+"^{tree}")
		if err != nil {
			return SourceEvidence{}, false, err
		}
		effective, err := rendered.Effective(ctx, rendered.ObjectStore{Dir: b.target}, rendered.BaseTree(tree))
		if err != nil {
			return SourceEvidence{}, false, fmt.Errorf("compute the effective tree of %s: %w", commit, err)
		}
		defer effective.Close()
		return SourceEvidence{AcceptedCommit: commit, EffectiveTree: effective.ID, PatchesDigest: effective.PatchesDigest}, true, nil
	}
	return SourceEvidence{}, false, nil
}
