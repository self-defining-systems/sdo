package broker

import (
	"context"
	"errors"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
)

// checkCandidate applies the same complete pipeline as memory check. Only the
// compile transport differs: the broker requires its independent validator Job.
func checkCandidate(ctx context.Context, root, base string, actor Actor, phase Phase, paths []string, runner Validator, publication *validate.PublicationContext, decision *contracts.ReflectionDecision, metricsDetectors bool) ([]validate.Diagnostic, error) {
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
	return validate.Check(ctx, validate.Request{Root: root, Base: base, Actor: actor, Phase: phase, Publication: publication, Decision: decision, MetricsDetectors: metricsDetectors}, compile)
}

// PublishReflection binds the returned worker decision independently of any
// self-check metadata the agent wrote in its proposal checkout.
func (b *Broker) PublishReflection(ctx context.Context, incidentID string, decision *contracts.ReflectionDecision) (Receipt, error) {
	return b.publish(ctx, incidentID, Responder, ReflectionPhase, "", nil, decision)
}
