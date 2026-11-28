package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
}

// HeadToDeploy reports the accepted head when its effective source differs
// from the newest recorded deployment or the cluster was last moved to a
// revision that is not accepted, after syncing the memory remote; an
// install with no recorded deployment (a seeded one) is measured against the
// source it adopted (seedBaselineLocked). A memory-only head changes no
// effective tree and needs no deployment.
func (b *Broker) HeadToDeploy(ctx context.Context) (change HeadChange, needed bool, err error) {
	err = b.locked(func() error {
		if err := b.syncUpstream(ctx); err != nil {
			return err
		}
		change, needed, err = b.headToDeployLocked(ctx)
		return err
	})
	return change, needed, err
}

// HeadUndeployed reports whether the checked-out head carries an effective
// source no recorded deployment has applied (HeadToDeploy's answer without
// syncing the remote): a push that landed after the last deployment.
func (b *Broker) HeadUndeployed(ctx context.Context) (needed bool, err error) {
	err = b.locked(func() error {
		_, needed, err = b.headToDeployLocked(ctx)
		return err
	})
	return needed, err
}

func (b *Broker) headToDeployLocked(ctx context.Context) (change HeadChange, needed bool, err error) {
	recorded, found, err := b.acceptedEvidenceLocked(ctx)
	if err != nil {
		return change, false, err
	}
	seeded := !found
	if seeded {
		// A seeded install (install-existing) adopted what runs and
		// recorded no deployment. The source it adopted is SDO's own newest
		// commit; an engineer's push that changes the effective source
		// after it is a head to deploy like any other.
		if recorded, found, err = b.seedBaselineLocked(ctx); err != nil || !found {
			return change, false, err
		}
	}
	head, err := gitText(ctx, b.target, "rev-parse", "HEAD")
	if err != nil {
		return change, false, err
	}
	accepted := recorded.AcceptedCommit
	if seeded {
		accepted = ""
	}
	// The cluster runs another revision than the accepted one: the head is to
	// deploy even when it returns to the accepted source.
	target, err := b.unacceptedTargetLocked(accepted)
	if err != nil {
		return change, false, err
	}
	if recorded.AcceptedCommit == head && target == nil {
		return change, false, nil
	}
	tree, err := gitText(ctx, b.target, "rev-parse", head+"^{tree}")
	if err != nil {
		return change, false, err
	}
	effective, err := rendered.Effective(ctx, rendered.ObjectStore{Dir: b.target}, rendered.BaseTree(tree))
	if err != nil {
		return change, false, fmt.Errorf("compute the effective tree of head %s: %w", head, err)
	}
	if target == nil && effective.ID == recorded.EffectiveTree {
		return change, false, nil
	}
	return HeadChange{Commit: head, EffectiveTree: effective.ID}, true, nil
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
// independent verifier cleared commit, an accepted head whose source no
// recorded deployment covers (someone else's commit, or SDO's own on top of
// one). The evidence must be of exactly that commit's tree and effective
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
		evidence.AcceptedCommit = commit
		if err := b.writeSourceEvidence(headDeploymentID(commit), evidence); err != nil {
			return err
		}
		return b.clearTarget()
	})
}

// headDeployed reports whether commit, a branch head the head deployer applied
// (someone else's commit, or SDO's own on top of one), is a recorded head
// deployment.
func (b *Broker) headDeployed(ctx context.Context, commit string) (bool, error) {
	return b.deployedBy(ctx, headDeploymentID(commit), commit)
}

// seedAnchorFile, in the broker workspace, holds the commit a seeded install
// adopted: the newest commit SDO wrote or installed when the head was first
// measured. It is written once and survives controller restarts.
const seedAnchorFile = "seed-adoption.json"

// seedBaselineLocked is the reference of an install with no recorded deployment:
// the effective source of the commit it adopted, carried forward over SDO's own
// later commits (one that carries an SDO-Actor trailer; someone else's commit
// never does) up to the first commit someone else wrote. A broker commit
// published on top of an engineer's undeployed push carries that push's
// source, so it is never the reference; with no such push the reference is
// SDO's newest commit. found is false when the history holds no SDO commit.
func (b *Broker) seedBaselineLocked(ctx context.Context) (SourceEvidence, bool, error) {
	anchor, err := b.seedAnchorLocked(ctx)
	if err != nil || anchor == "" {
		return SourceEvidence{}, false, err
	}
	out, err := runGit(ctx, b.target, "log", "--first-parent", "--reverse", "--format=%H%x1f%(trailers:key=SDO-Actor,valueonly)%x1e", anchor+"..HEAD")
	if err != nil {
		return SourceEvidence{}, false, err
	}
	reference := anchor
	for _, record := range strings.Split(string(out), "\x1e") {
		fields := strings.Split(strings.TrimSpace(record), "\x1f")
		if len(fields) != 2 || !validGitID(strings.TrimSpace(fields[0])) {
			continue
		}
		if strings.TrimSpace(fields[1]) == "" {
			break
		}
		reference = strings.TrimSpace(fields[0])
	}
	tree, err := gitText(ctx, b.target, "rev-parse", reference+"^{tree}")
	if err != nil {
		return SourceEvidence{}, false, err
	}
	effective, err := rendered.Effective(ctx, rendered.ObjectStore{Dir: b.target}, rendered.BaseTree(tree))
	if err != nil {
		return SourceEvidence{}, false, fmt.Errorf("compute the effective tree of %s: %w", reference, err)
	}
	return SourceEvidence{AcceptedCommit: reference, EffectiveTree: effective.ID}, true, nil
}

// seedAnchorLocked is the commit a seeded install adopted: the recorded one
// while it is still in the accepted history, else SDO's newest commit, which it
// records. "" when the history holds no SDO commit.
func (b *Broker) seedAnchorLocked(ctx context.Context) (string, error) {
	path := filepath.Join(b.workspaceRoot, seedAnchorFile)
	var recorded struct {
		Commit string `json:"commit"`
	}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if json.Unmarshal(data, &recorded) == nil && validGitID(recorded.Commit) {
			if _, err := runGit(ctx, b.target, "merge-base", "--is-ancestor", recorded.Commit, "HEAD"); err == nil {
				return recorded.Commit, nil
			}
		}
	case !errors.Is(err, os.ErrNotExist):
		return "", err
	}
	out, err := runGit(ctx, b.target, "log", "--first-parent", "--format=%H%x1f%(trailers:key=SDO-Actor,valueonly)%x1e", "HEAD")
	if err != nil {
		return "", err
	}
	for _, record := range strings.Split(string(out), "\x1e") {
		fields := strings.Split(strings.TrimSpace(record), "\x1f")
		if len(fields) != 2 || strings.TrimSpace(fields[1]) == "" || !validGitID(strings.TrimSpace(fields[0])) {
			continue
		}
		recorded.Commit = strings.TrimSpace(fields[0])
		if data, err = json.Marshal(recorded); err != nil {
			return "", err
		}
		return recorded.Commit, writeDurable(path, data)
	}
	return "", nil
}
