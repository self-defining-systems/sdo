package broker

import (
	"context"
	"errors"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/telemetry"
)

// checkCandidate applies the same complete pipeline as memory check. Only the
// compile transport differs: the broker requires its bounded validation subprocess.
func (b *Broker) checkCandidate(ctx context.Context, root, base string, actor Actor, phase Phase, paths []string, runner Validator, publication *validate.PublicationContext, decision *contracts.ReflectionDecision, gaps *[]validate.Diagnostic) ([]validate.Diagnostic, error) {
	var compile validate.CompileRunner
	if runner != nil {
		compile = func(ctx context.Context, request validate.Request) ([]validate.Diagnostic, error) {
			ctx = context.WithValue(ctx, compileBaselineKey{}, request.HealthyBaseline)
			ctx = context.WithValue(ctx, replayKey{}, request.Replay)
			ctx = context.WithValue(ctx, telemetryPolicyKey{}, request.Telemetry)
			var jobGaps []validate.Diagnostic
			ctx = context.WithValue(ctx, coverageGapsKey{}, &jobGaps)
			// request.Root is the candidate, or the accepted base tree Check
			// replays to establish pre-existing healthy-baseline faults.
			err := runner(ctx, request.Root, paths)
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

// telemetryPolicyKey carries the check's telemetry policy to the compile
// transport, so the validation subprocess resolves detector queries against
// the same effective sources the broker's static layer and the agent's
// self-check use.
type telemetryPolicyKey struct{}

// ValidationTelemetryPolicy is the telemetry policy of the check a Validator
// is running for.
func ValidationTelemetryPolicy(ctx context.Context) (telemetry.Policy, bool) {
	policy, ok := ctx.Value(telemetryPolicyKey{}).(telemetry.Policy)
	return policy, ok
}

// validationRequest is the single source of all acceptance inputs. Attestations
// serialize this same request so future policy fields cannot be omitted.
func (b *Broker) validationRequest(root, base string, actor Actor, phase Phase, publication *validate.PublicationContext, decision *contracts.ReflectionDecision, gaps *[]validate.Diagnostic) validate.Request {
	return validate.Request{Replay: b.replay, CodePath: b.codePath, ObservationCeiling: b.observationCeiling, ApplicationNamespace: b.applicationNamespace, Root: root, Base: base, Actor: actor, Phase: phase, Publication: publication, Decision: decision, CoverageGaps: gaps, RepairVerified: b.RepairVerified, PlanCatalogs: b.PlanCatalogs, Telemetry: b.telemetry.Policy, TelemetryRegistry: b.telemetry.Registry}
}

// coverageGapsKey carries the broker's collector for the validator's gaps.
type coverageGapsKey struct{}

// replayNotesKey carries the collector for advisory mitigation replay notes.
type replayNotesKey struct{}

func compileBaseline(ctx context.Context) string {
	baseline, _ := ctx.Value(compileBaselineKey{}).(string)
	return baseline
}
