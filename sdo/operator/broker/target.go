package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

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
	// Interrupted names the phases whose objects are refused a restore;
	// RefusesAll is true when they are all of its phases.
	Interrupted string
	RefusesAll  bool
}

func unacceptedTarget(t *rendered.DeploymentTarget) Target {
	return Target{Commit: t.Commit, Unaccepted: true, Incomplete: len(t.Unreached)+len(t.Interrupted) > 0,
		Interrupted: strings.Join(t.Interrupted, ", "), RefusesAll: len(t.Interrupted) > 0 && len(t.Interrupted) == len(t.Phases)}
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
// publisher holding its lock. When that fails it returns, with the error, the
// recorded target read without the lock: an acceptance drops a target it
// supersedes, so a recorded one is what the cluster was last moved to. The
// controller uses it and retries.
func (b *Broker) TryTarget(ctx context.Context) (out Target, err error) {
	err = b.tryLocked(func() error {
		evidence, _, err := b.acceptedEvidenceLocked(ctx)
		if err != nil {
			return err
		}
		target, err := b.unacceptedTargetLocked(evidence.AcceptedCommit)
		out = Target{Commit: evidence.AcceptedCommit}
		if target != nil {
			out = unacceptedTarget(target)
		}
		return err
	})
	if err != nil {
		out = Target{}
		if target, readErr := b.DeploymentTarget(); readErr == nil && target != nil {
			out = unacceptedTarget(target)
		}
	}
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
			phases := write.Later
			if write.Phase != "" {
				phases = append([]string{write.Phase}, phases...)
			}
			target = &rendered.DeploymentTarget{AcceptedCommit: accepted, Previous: previous, Phases: phases, Unreached: slices.Clone(phases)}
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

// clearSupersededTargetLocked drops a target a later acceptance superseded,
// and keeps one recorded since.
func (b *Broker) clearSupersededTargetLocked(ctx context.Context) error {
	evidence, _, err := b.acceptedEvidenceLocked(ctx)
	if err != nil {
		return err
	}
	if target, err := b.unacceptedTargetLocked(evidence.AcceptedCommit); err != nil || target != nil {
		return err
	}
	return b.clearTarget()
}

// clearTarget drops the target once a deployment is accepted.
func (b *Broker) clearTarget() error {
	if err := os.Remove(filepath.Join(b.workspaceRoot, targetFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("clear the deployment target: %w", err)
	}
	return nil
}
