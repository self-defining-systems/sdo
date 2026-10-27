package broker

import (
	"context"
	"errors"

	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/rendered"
)

// RecordRenderedLock accepts only a controller-produced deployment snapshot,
// independently checked against a fresh render under the publication lock.
// The expected head prevents a rollout observation from racing source acceptance.
func (b *Broker) RecordRenderedLock(ctx context.Context, expected string, snapshot rendered.Snapshot) (string, error) {
	var commit string
	err := b.locked(func() error {
		if err := b.requireClean(ctx); err != nil {
			return err
		}
		head, err := gitText(ctx, b.target, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		if head != expected {
			return errors.New("rendered lock accepted head changed")
		}
		if err := snapshot.Validate(ctx, b.target, head); err != nil {
			return err
		}
		stage, cleanup, err := b.privateClone(ctx)
		if err != nil {
			return err
		}
		defer cleanup()
		if err := snapshot.Write(stage); err != nil {
			return err
		}
		paths, err := validate.ChangedPaths(ctx, stage, head)
		if err != nil {
			return err
		}
		if len(paths) == 0 {
			commit = head
			return nil
		}
		receipt, err := b.accept(ctx, stage, "rendered-"+snapshot.Lock.SourceCommit, "controller", Phase("rendered"), paths)
		commit = receipt.Commit
		return err
	})
	return commit, err
}

// Head returns the current broker-accepted revision, including a publication
// whose reply was lost before the controller persisted its next revision.
func (b *Broker) Head(ctx context.Context) (string, error) {
	var head string
	err := b.locked(func() error { var err error; head, err = gitText(ctx, b.target, "rev-parse", "HEAD"); return err })
	return head, err
}
