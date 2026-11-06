package operatorcli

import (
	"encoding/json"
	"slices"
	"testing"

	"sdo.dev/operator/install"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/telemetry"
)

func TestInstallTelemetryPolicyFollowsTheObservabilityStack(t *testing.T) {
	on, err := installTelemetryPolicy(install.Config{BackendRecipes: telemetrysources.Recipes(), TelemetryProfile: mustBuiltin(t, "prometheus-default", "sdo-ctl")}, "shop", "sdo-ctl")
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
	off, err := installTelemetryPolicy(install.Config{BackendRecipes: telemetrysources.Recipes(), TelemetryProfile: mustBuiltin(t, "none", "sdo-ctl")}, "shop", "sdo-ctl")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := off.Recipe("prometheus"); ok || off.Granted(telemetry.CapPodDiscovery) || off.Granted(telemetry.CapNodeProxy) {
		t.Fatalf("without the stack no recipe or cluster read is granted: %+v", off)
	}
}

func TestMemoryTelemetryPolicyRoundTripsTheBrokersPolicy(t *testing.T) {
	want, err := installTelemetryPolicy(install.Config{BackendRecipes: telemetrysources.Recipes(), TelemetryProfile: mustBuiltin(t, "prometheus-default", "sdo-ctl")}, "shop", "sdo-ctl")
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

func TestInstallTelemetryPolicyProvidesTheProfileSources(t *testing.T) {
	profile := withSource(t, mustBuiltin(t, "none", "sdo-ctl"), "ops=http://prom.ops.svc:9090")
	policy, err := installTelemetryPolicy(install.Config{BackendRecipes: telemetrysources.Recipes(), TelemetryProfile: profile}, "shop", "sdo-ctl")
	if err != nil {
		t.Fatal(err)
	}
	if len(policy.Provided) != 1 {
		t.Fatalf("provided = %+v", policy.Provided)
	}
	got := policy.Provided[0]
	if got.ID != "ops" || !got.Default || got.Origin != telemetry.OriginOperator || !got.Serves(telemetry.KindMetrics) {
		t.Fatalf("source = %+v", got)
	}
	if !slices.Contains(got.Capabilities, telemetry.CapAlerts) {
		t.Fatalf("the prometheus adapter serves alerts, so the source must: %+v", got)
	}
	bad := telemetry.Profile{Name: "bad", Sources: []telemetry.ProfileSource{{ID: "x", Adapter: "nosuch", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Endpoint: "http://x.ops.svc"}}}
	if _, err := installTelemetryPolicy(install.Config{TelemetryProfile: bad}, "shop", "sdo-ctl"); err == nil {
		t.Fatal("an unknown adapter must fail, not silently drop the source")
	}
	missing := telemetry.Profile{Name: "r", Recipes: []string{"nosuch"}}
	if _, err := installTelemetryPolicy(install.Config{BackendRecipes: telemetrysources.Recipes(), TelemetryProfile: missing}, "shop", "sdo-ctl"); err == nil {
		t.Fatal("an unknown recipe must fail")
	}
}
