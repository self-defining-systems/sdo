package broker

import (
	"context"

	"sdo.dev/operator/memory/validate"
)

// validateCandidate runs the shared validator's read-only layers on a staged
// candidate against the accepted base commit. Detector compilation executes
// untrusted code, so it runs in the isolated validation Job (see Validator),
// which invokes the same validator's compile layer.
func validateCandidate(ctx context.Context, candidate, base string, actor Actor, phase Phase) error {
	return validateLayers(ctx, candidate, base, actor, phase, validate.Static)
}

// validateOwnership checks only who may change which paths.
func validateOwnership(ctx context.Context, checkout, base string, actor Actor, phase Phase) error {
	return validateLayers(ctx, checkout, base, actor, phase, validate.Ownership)
}

func validateLayers(ctx context.Context, root, base string, actor Actor, phase Phase, layers validate.Layer) error {
	diagnostics, err := validate.Validate(ctx, validate.Request{Root: root, Base: base, Actor: actor, Phase: phase, Layers: layers})
	if err != nil {
		return err
	}
	return validate.Err(diagnostics)
}
