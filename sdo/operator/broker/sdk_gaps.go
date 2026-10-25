package broker

import (
	"context"
	"fmt"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
)

// RecordSDKGaps appends reflection's exact requested operations to the incident
// outcome. It cannot alter classification, receipts or verification evidence.
// Repeated delivery is idempotent, including a crash after the Git commit.
func (b *Broker) RecordSDKGaps(ctx context.Context, id string, gaps []*contracts.SdkGap) (Receipt, error) {
	var receipt Receipt
	err := b.locked(func() error {
		if err := b.requireClean(ctx); err != nil {
			return err
		}
		stage, cleanup, err := b.privateClone(ctx)
		if err != nil {
			return err
		}
		defer cleanup()
		base, err := gitText(ctx, stage, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		if _, err = memory.NewJSONStore(stage).AppendSDKGaps(ctx, id, gaps); err != nil {
			return err
		}
		paths, err := validate.ChangedPaths(ctx, stage, base)
		if err != nil {
			return err
		}
		if len(paths) == 0 {
			prior, ok, findErr := b.findAccepted(ctx, id, OutcomePhase)
			if findErr != nil {
				return findErr
			}
			if !ok {
				return fmt.Errorf("SDK gap outcome has no attributed broker commit")
			}
			receipt = prior
			return nil
		}
		if len(paths) != 1 || paths[0] != layout.OutcomePath(id) {
			return fmt.Errorf("SDK gap annotation changed unrelated evidence")
		}
		receipt, err = b.accept(ctx, stage, id, "controller", OutcomePhase, paths)
		return err
	})
	return receipt, err
}
