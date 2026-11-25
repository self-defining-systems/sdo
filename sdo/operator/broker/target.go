package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"sdo.dev/operator/rendered"
)

// targetFile, in the broker workspace, holds the revision the cluster was last
// moved to while it is not accepted. Acceptance and memory never read it.
const targetFile = "deployment-target.json"

// Target is the revision the cluster was last moved to.
type Target struct {
	// Commit is the target's commit, else the newest accepted deployment's.
	Commit string
	// Unaccepted is true while the target is not the accepted deployment.
	Unaccepted bool
	// Incomplete is true when some of its phases were not applied whole.
	Incomplete bool
}

// DeploymentTarget reads the recorded target without a lock (nil: none).
func (b *Broker) DeploymentTarget() (*rendered.DeploymentTarget, error) {
	data, err := os.ReadFile(filepath.Join(b.workspaceRoot, targetFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var target rendered.DeploymentTarget
	if err := json.Unmarshal(data, &target); err != nil {
		return nil, fmt.Errorf("decode the deployment target: %w", err)
	}
	if !validGitID(target.Commit) || !validGitID(target.Pins.EffectiveTree) {
		return nil, errors.New("the deployment target names no commit or effective tree")
	}
	return &target, nil
}

// unacceptedTargetLocked is the recorded target unless accepted, the newest
// accepted deployment's commit, is it or superseded it.
func (b *Broker) unacceptedTargetLocked(accepted string) (*rendered.DeploymentTarget, error) {
	target, err := b.DeploymentTarget()
	if err != nil || target == nil || target.Commit == accepted || target.AcceptedCommit != accepted {
		return nil, err
	}
	return target, nil
}

// TryTarget reads the target and the deployment ledger without waiting for a
// publisher holding its lock. The controller retries on lock contention.
func (b *Broker) TryTarget(ctx context.Context) (out Target, err error) {
	err = b.tryLocked(func() error {
		evidence, _, err := b.acceptedEvidenceLocked(ctx)
		if err != nil {
			return err
		}
		target, err := b.unacceptedTargetLocked(evidence.AcceptedCommit)
		out = Target{Commit: evidence.AcceptedCommit}
		if target != nil {
			out = Target{Commit: target.Commit, Unaccepted: true, Incomplete: len(target.Unreached)+len(target.Interrupted) > 0}
		}
		return err
	})
	return out, err
}

// RecordTarget durably records one step of the deployment of commit, before
// its write to the cluster or once that write succeeded.
func (b *Broker) RecordTarget(ctx context.Context, commit string, write rendered.TargetWrite) error {
	if !validGitID(commit) || !validGitID(write.Pins.EffectiveTree) {
		return errors.New("a deployment target needs a full commit and effective tree ID")
	}
	return b.locked(func() error {
		evidence, found, err := b.acceptedEvidenceLocked(ctx)
		if err != nil {
			return err
		}
		accepted := evidence.AcceptedCommit
		target, err := b.unacceptedTargetLocked(accepted)
		if err != nil {
			return err
		}
		if target == nil || target.Pins.EffectiveTree != write.Pins.EffectiveTree {
			previous := target
			if previous == nil {
				if !found {
					if evidence, _, err = b.seedBaselineLocked(ctx); err != nil {
						return err
					}
				}
				if evidence.AcceptedCommit != "" {
					previous = &rendered.DeploymentTarget{Commit: evidence.AcceptedCommit}
				}
			}
			target = &rendered.DeploymentTarget{AcceptedCommit: accepted, Previous: previous, Unreached: append([]string{write.Phase}, write.Later...)}
		}
		target.Commit = commit
		target.Advance(write)
		data, err := json.Marshal(target)
		if err != nil {
			return err
		}
		return writeDurable(filepath.Join(b.workspaceRoot, targetFile), data)
	})
}

// clearTarget drops the target once a deployment is accepted.
func (b *Broker) clearTarget() error {
	if err := os.Remove(filepath.Join(b.workspaceRoot, targetFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("clear the deployment target: %w", err)
	}
	return nil
}
