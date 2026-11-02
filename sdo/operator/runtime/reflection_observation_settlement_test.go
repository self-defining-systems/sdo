package runtime

import (
	"context"
	"fmt"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type settle3DeletionProgram struct {
	uncachedPolicyProgram
	kind string
}

func (p settle3DeletionProgram) Spec() program.ProgramSpec {
	spec := p.uncachedPolicyProgram.Spec()
	spec.Params[0].Kind = program.Kind(p.kind)
	return spec
}

type settle3DeletionRollout struct{ client *lifecycleTestClient }

func (r settle3DeletionRollout) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	for _, ref := range refs {
		if ref.Kind == "Deployment" {
			if _, err := r.client.Get(ctx, ref); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r settle3DeletionRollout) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	return r.Wait(ctx, refs)
}

type lifecycleObservationFailure struct{}

func (lifecycleObservationFailure) AwaitWrites(context.Context, []ObservedWrite) error {
	return fmt.Errorf("write observation unknown: synthetic informer unavailable")
}
