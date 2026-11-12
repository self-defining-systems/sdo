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
	return b.recordReflectionAnnotations(ctx, id, gaps, nil)
}

// RecordReflectionRejections appends named broker validation decisions exactly
// once. Only the storage edge can materialize the outcome document.
func (b *Broker) RecordReflectionRejections(ctx context.Context, id string, rejections []*contracts.ProposalRejection) (Receipt, error) {
	return b.recordReflectionAnnotations(ctx, id, nil, rejections)
}

func (b *Broker) recordReflectionAnnotations(ctx context.Context, id string, gaps []*contracts.SdkGap, rejections []*contracts.ProposalRejection) (Receipt, error) {
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
		store := memory.NewJSONStore(stage)
		if _, err = store.AppendSDKGaps(ctx, id, gaps); err != nil {
			return err
		}
		if _, err = store.AppendReflectionRejections(ctx, id, rejections); err != nil {
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
				return fmt.Errorf("reflection annotation outcome has no attributed broker commit")
			}
			receipt = prior
			return nil
		}
		if len(paths) != 1 || paths[0] != layout.OutcomePath(id) {
			return fmt.Errorf("reflection annotation changed unrelated evidence")
		}
		receipt, err = b.accept(ctx, stage, id, "controller", OutcomePhase, paths)
		return err
	})
	return receipt, err
}

// RecordSourceRepair appends what became of the responder's committed source
// to the incident's outcome, in its own broker commit. Only the storage edge
// can materialize the outcome document; the classification and every other
// piece of evidence stay as the closure committed them.
func (b *Broker) RecordSourceRepair(ctx context.Context, id string, repair *contracts.SourceRepair) (Receipt, error) {
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
		if _, err = memory.NewJSONStore(stage).SetSourceRepair(ctx, id, repair); err != nil {
			return err
		}
		paths, err := validate.ChangedPaths(ctx, stage, base)
		if err != nil {
			return err
		}
		if len(paths) == 0 {
			// Already recorded: report the outcome's commit.
			prior, ok, findErr := b.findAccepted(ctx, id, OutcomePhase)
			if findErr != nil {
				return findErr
			}
			if !ok {
				return fmt.Errorf("source repair outcome has no attributed broker commit")
			}
			receipt = prior
			return nil
		}
		if len(paths) != 1 || paths[0] != layout.OutcomePath(id) {
			return fmt.Errorf("source repair annotation changed unrelated evidence")
		}
		receipt, err = b.accept(ctx, stage, id, "controller", OutcomePhase, paths)
		return err
	})
	return receipt, err
}
