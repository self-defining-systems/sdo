package broker

import (
	"context"
	"errors"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
)

// checkCandidate applies the same complete pipeline as memory check. Only the
<<<<<<< HEAD
// compile transport differs: the broker requires its bounded validation subprocess.
func checkCandidate(ctx context.Context, root, base string, actor Actor, phase Phase, paths []string, runner Validator, publication *validate.PublicationContext, decision *contracts.ReflectionDecision, metricsDetectors bool) ([]validate.Diagnostic, error) {
=======
// compile transport differs: the broker requires its independent validator Job.
func (b *Broker) checkCandidate(ctx context.Context, root, base string, actor Actor, phase Phase, paths []string, runner Validator, publication *validate.PublicationContext, decision *contracts.ReflectionDecision, gaps *[]validate.Diagnostic) ([]validate.Diagnostic, error) {
>>>>>>> origin/vic-lsh/dev/sdo-improve
	var compile validate.CompileRunner
	if runner != nil {
		compile = func(ctx context.Context, request validate.Request) ([]validate.Diagnostic, error) {
			ctx = context.WithValue(ctx, compileBaselineKey{}, request.HealthyBaseline)
			ctx = context.WithValue(ctx, replayKey{}, request.Replay)
			var jobGaps []validate.Diagnostic
			ctx = context.WithValue(ctx, coverageGapsKey{}, &jobGaps)
			err := runner(ctx, root, paths)
			var failure *validate.Failure
			if errors.As(err, &failure) {
				return failure.Diagnostics, nil
			}
			if err == nil && request.CoverageGaps != nil {
				*request.CoverageGaps = append(*request.CoverageGaps, jobGaps...)
			}
			return nil, err
		}
	}
	request := b.validationRequest(root, base, actor, phase, publication, decision, gaps)
	diagnostics, err := validate.Check(ctx, request, compile)
	if err != nil || len(diagnostics) > 0 || b.codePath != "shell" || compile == nil {
		return diagnostics, err
	}
	// The shell plugin owns its mechanism; the broker independently retains the grant table.
	return compile(ctx, request)
}

// PublishReflection binds the returned worker decision independently of any
// self-check metadata the agent wrote in its proposal checkout.
func (b *Broker) PublishReflection(ctx context.Context, incidentID string, decision *contracts.ReflectionDecision) (Receipt, error) {
	return b.publish(ctx, incidentID, Responder, ReflectionPhase, "", nil, decision)
}

type compileBaselineKey struct{}

type replayKey struct{}

// validationRequest is the single source of all acceptance inputs. Attestations
// serialize this same request so future policy fields cannot be omitted.
func (b *Broker) validationRequest(root, base string, actor Actor, phase Phase, publication *validate.PublicationContext, decision *contracts.ReflectionDecision, gaps *[]validate.Diagnostic) validate.Request {
 return validate.Request{Replay:b.replay,CodePath:b.codePath,ObservationCeiling:b.observationCeiling,ApplicationNamespace:b.applicationNamespace,Root:root,Base:base,Actor:actor,Phase:phase,Publication:publication,Decision:decision,MetricsDetectors:b.MetricsDetectors,CoverageGaps:gaps}
}
