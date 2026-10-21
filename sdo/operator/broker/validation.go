package broker

import (
	"context"
	"errors"

	"sdo.dev/operator/memory/validate"
)

// validateOwnership checks only who may change which paths.
func validateOwnership(ctx context.Context, checkout, base string, actor Actor, phase Phase) error {
	diagnostics, err := validate.Validate(ctx, validate.Request{Root: checkout, Base: base, Actor: actor, Phase: phase, Layers: validate.Ownership})
	if err != nil {
		return err
	}
	return validate.Err(diagnostics)
}

// checkCandidate applies the same complete pipeline as memory check. Only the
// compile transport differs: the broker requires its independent validator Job.
func checkCandidate(ctx context.Context, root, base string, actor Actor, phase Phase, paths []string, runner Validator) ([]validate.Diagnostic, error) {
	var compile validate.CompileRunner
	if runner != nil {
		compile = func(ctx context.Context, _ validate.Request) ([]validate.Diagnostic, error) {
			err := runner(ctx, root, paths)
			var failure *validate.Failure
			if errors.As(err, &failure) {
				return failure.Diagnostics, nil
			}
			return nil, err
		}
	}
	return validate.Check(ctx, validate.Request{Root: root, Base: base, Actor: actor, Phase: phase}, compile)
}
