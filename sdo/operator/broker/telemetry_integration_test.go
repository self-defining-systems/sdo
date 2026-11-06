//go:build integration

package broker

import (
	"context"
	"reflect"
	"testing"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/telemetry"
)

// The broker's compile transport receives the policy its own check runs
// under, so the validation subprocess and the static layer see one effective
// source set.
func TestBrokerHandsItsTelemetryPolicyToTheCompileValidator(t *testing.T) {
	w := newOnboardedWorld(t, false)
	policy := telemetry.Policy{Mode: telemetry.ModeOpen, ControlNamespace: "sdo-ctl", ApplicationNamespace: "shop",
		Provided: []telemetry.Source{{ID: "prom", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Adapter: "prometheus", Default: true, Capabilities: []telemetry.Capability{telemetry.CapAlerts}, Origin: telemetry.OriginOperator}}}
	w.broker.telemetry.Policy = policy
	var seen []telemetry.Policy
	w.broker.validator = func(ctx context.Context, _ string, _ []string) error {
		got, ok := ValidationTelemetryPolicy(ctx)
		if !ok {
			t.Error("compile validator ran without the check's telemetry policy")
		}
		seen = append(seen, got)
		return nil
	}
	p, err := w.broker.CreateProposalForPhase(context.Background(), "goal-2", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{layout.GoalFile: advanceGoal + "\nRevised\n"})
	if _, err := w.broker.StageCandidate(context.Background(), "goal-2", Human, InitialPhase); err != nil {
		t.Fatal(err)
	}
	if len(seen) == 0 {
		t.Fatal("the candidate never reached the compile validator; the test proves nothing")
	}
	for _, got := range seen {
		if !reflect.DeepEqual(got, policy) {
			t.Fatalf("validator policy = %+v, want the broker's %+v", got, policy)
		}
	}
}
