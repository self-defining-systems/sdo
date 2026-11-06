package operatorcli

import (
	"encoding/json"
	"slices"
	"testing"

	"sdo.dev/operator/install"
	"sdo.dev/operator/telemetry"
)

func TestInstallTelemetryPolicyFollowsTheObservabilityStack(t *testing.T) {
	on, err := installTelemetryPolicy(install.Config{}, "shop", "sdo-ctl")
	if err != nil {
		t.Fatal(err)
	}
	if err := on.Validate(); err != nil {
		t.Fatal(err)
	}
	if on.ControlNamespace != "sdo-ctl" || on.ApplicationNamespace != "shop" || on.EffectiveMode() != telemetry.ModeOpen {
		t.Fatalf("policy = %+v", on)
	}
	recipe, ok := on.Recipe("prometheus")
	if !ok || !on.Granted(telemetry.CapNodeProxy) || len(recipe.Declares) != 2 {
		t.Fatalf("with the stack, the prometheus recipe and its reads are granted: %+v", on)
	}
	off, err := installTelemetryPolicy(install.Config{DisableObservability: true}, "shop", "sdo-ctl")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := off.Recipe("prometheus"); ok || off.Granted(telemetry.CapPodDiscovery) || off.Granted(telemetry.CapNodeProxy) {
		t.Fatalf("without the stack no recipe or cluster read is granted: %+v", off)
	}
}

func TestMemoryTelemetryPolicyRoundTripsTheBrokersPolicy(t *testing.T) {
	want, err := installTelemetryPolicy(install.Config{}, "shop", "sdo-ctl")
	if err != nil {
		t.Fatal(err)
	}
	want.Provided = []telemetry.Source{{ID: "ops", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Adapter: "prometheus", Default: true, Origin: telemetry.OriginOperator}}
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := memoryTelemetryPolicy(func(string) string { return string(raw) }, "ignored")
	if err != nil {
		t.Fatal(err)
	}
	again, _ := json.Marshal(got)
	if string(again) != string(raw) {
		t.Fatalf("policy changed across the environment:\n got %s\nwant %s", again, raw)
	}
}

func TestMemoryTelemetryPolicyDefaultsAndRejects(t *testing.T) {
	got, err := memoryTelemetryPolicy(func(string) string { return "" }, "shop")
	if err != nil || got.ApplicationNamespace != "shop" || got.EffectiveMode() != telemetry.ModeOpen {
		t.Fatalf("default = %+v, %v", got, err)
	}
	for _, bad := range []string{"{", `{"mode":"closed"}`, `{"externalHosts":["10.0.0.1"]}`} {
		if _, err := memoryTelemetryPolicy(func(string) string { return bad }, "shop"); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestInstallTelemetryPolicyProvidesTheMetricsURLAsTheDefaultSource(t *testing.T) {
	policy, err := installTelemetryPolicy(install.Config{MetricsURL: "http://prom.ops.svc:9090"}, "shop", "sdo-ctl")
	if err != nil {
		t.Fatal(err)
	}
	if len(policy.Provided) != 1 {
		t.Fatalf("provided = %+v", policy.Provided)
	}
	got := policy.Provided[0]
	if got.ID != "default" || !got.Default || got.Origin != telemetry.OriginOperator || !got.Serves(telemetry.KindMetrics) {
		t.Fatalf("source = %+v", got)
	}
	if !slices.Contains(got.Capabilities, telemetry.CapAlerts) {
		t.Fatalf("the prometheus adapter serves alerts, so the source must: %+v", got)
	}
	if _, err := installTelemetryPolicy(install.Config{MetricsURL: "not a url"}, "shop", "sdo-ctl"); err == nil {
		t.Fatal("an unusable metrics URL must fail, not silently drop the source")
	}
}
