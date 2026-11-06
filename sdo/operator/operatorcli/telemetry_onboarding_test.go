package operatorcli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/telemetry"
)

// The PR-tier form of the onboarding invariant (the nightly form compiles the
// diagnostics module, about a minute): the judge's proposal under every profile
// is staged on the exact onboarding broker, with the compile step replaced by
// the part of it that failed in the live onboarding: the validation child's
// request is built through the subprocess's own argument encoding, and the
// effective telemetry sources it resolves must serve what Go installed. A child
// request that loses the policy or the registry fails here as it did live
// (telemetry.unbound[metrics-alerts], or no adapter registry for a binding),
// without building any detector.
func TestOnboardingBrokerAcceptsTheJudgesProposalUnderEveryProfileWithoutCompiling(t *testing.T) {
	registry := onboardingTestRegistry()
	compiles := 0
	runOnboardingCases(t, func(ctx context.Context, candidate string, _ []string) error {
		compiles++
		request, err := broker.ChildRequest(ctx, candidate, registry)
		if err != nil {
			return err
		}
		sources, err := validate.EffectiveTelemetrySources(request.Root, request.Telemetry, request.TelemetryRegistry)
		if err != nil {
			return fmt.Errorf("telemetry.schema: the child cannot compute the effective sources: %w", err)
		}
		bound := make([]telemetry.Bound, len(sources))
		for i, source := range sources {
			bound[i] = telemetry.Bound{Source: source}
		}
		resolver, err := telemetry.NewResolver(bound)
		if err != nil {
			return fmt.Errorf("telemetry.unbound: %w", err)
		}
		if _, err := os.Stat(filepath.Join(candidate, layout.DetectorDir(layout.ClassHealth, memory.MetricsAlertsDetectorID))); err == nil && len(resolver.AlertSources()) == 0 {
			return fmt.Errorf("telemetry.unbound[%s]: reads alerts but no bound source serves alerts", memory.MetricsAlertsDetectorID)
		}
		return nil
	})
	if compiles == 0 {
		t.Fatal("the stub validator never ran; the cases do not reach the compile layer")
	}
}
