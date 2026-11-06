package operatorcli

import (
	"encoding/json"
	"testing"

	"sdo.dev/operator/install"
	"sdo.dev/operator/telemetry"
)

func TestInstallTelemetryPolicyFollowsTheObservabilityStack(t *testing.T) {
	on := installTelemetryPolicy(install.Config{}, "shop", "sdo-ctl")
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
	off := installTelemetryPolicy(install.Config{DisableObservability: true}, "shop", "sdo-ctl")
	if _, ok := off.Recipe("prometheus"); ok || off.Granted(telemetry.CapPodDiscovery) || off.Granted(telemetry.CapNodeProxy) {
		t.Fatalf("without the stack no recipe or cluster read is granted: %+v", off)
	}
}

func TestMemoryTelemetryPolicyRoundTripsTheBrokersPolicy(t *testing.T) {
	want := installTelemetryPolicy(install.Config{}, "shop", "sdo-ctl")
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
