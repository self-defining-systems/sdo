package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"sdo.dev/operator/operability"
	"sdo.dev/operator/telemetryapply"
)

// TelemetryApplier is the trusted applier of the designer's telemetry
// backends. The coordinator runs it on the staged candidate after the live
// deployment checks and before the independent detector evaluation, so the
// detectors are evaluated against backends that are deployed and ready. Nil
// skips the phase: the environment then has no designer-deployable backends.
type TelemetryApplier interface {
	// Apply deploys the backends the checkout's backends file selects. A
	// *telemetryapply.Error is a correction for the designer; any other
	// error is an infrastructure failure.
	Apply(ctx context.Context, checkout string) (telemetryapply.Applied, error)
}

// telemetryApplyInfrastructure marks an apply failure that is not the
// designer's, so the bounded infrastructure retry applies.
type telemetryApplyInfrastructure struct{ err error }

func (e *telemetryApplyInfrastructure) Error() string {
	return "telemetry apply: " + e.err.Error()
}
func (e *telemetryApplyInfrastructure) Unwrap() error               { return e.err }
func (e *telemetryApplyInfrastructure) InfrastructureOutcome() bool { return true }

// applyTelemetry is the apply phase. It returns O3.telemetry: a pass recording
// what was applied (and no check when nothing was), or a fail routed to the
// designer by cause binding, naming the typed reason and the file to correct.
func (c Coordinator) applyTelemetry(ctx context.Context, checkout string) ([]operability.Check, error) {
	if c.TelemetryApply == nil {
		return nil, nil
	}
	const id = "O3.telemetry"
	applied, err := c.TelemetryApply.Apply(ctx, checkout)
	var typed *telemetryapply.Error
	switch {
	case errors.As(err, &typed):
		check := operability.Check{ID: id, Phase: operability.PhaseLive, Status: operability.StatusFail, Cause: operability.CauseBinding,
			Detail: operability.Clip(fmt.Sprintf("[%s] telemetry backend %q was not applied: %s", typed.Reason, typed.Backend, typed.Detail))}
		if typed.Path != "" {
			check.Paths = []string{typed.Path}
		}
		return []operability.Check{check}, nil
	case err != nil:
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &telemetryApplyInfrastructure{err: err}
	case len(applied.Backends) == 0:
		return nil, nil
	}
	return []operability.Check{{ID: id, Phase: operability.PhaseLive, Status: operability.StatusPass, Detail: operability.Clip(applied.Summary())}}, nil
}
