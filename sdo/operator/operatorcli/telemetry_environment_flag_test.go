package operatorcli

import (
	"reflect"
	"testing"

	"sdo.dev/operator/install"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/telemetry"
)

func TestControllerLaunchCarriesTheTelemetryEnvironmentFile(t *testing.T) {
	config, _, err := parseControllerLaunch(controllerLaunchArgs("--telemetry-environment", "/sdo/telemetry/environment.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got := config.Controller.GetTelemetryEnvironmentFile(); got != "/sdo/telemetry/environment.json" {
		t.Fatalf("telemetry environment file = %q", got)
	}
	plain, _, err := parseControllerLaunch(controllerLaunchArgs())
	if err != nil || plain.Controller.GetTelemetryEnvironmentFile() != "" {
		t.Fatalf("no flag = %q, %v", plain.Controller.GetTelemetryEnvironmentFile(), err)
	}
}

func TestControllerLaunchRefusesTheRetiredMetricsFlags(t *testing.T) {
	for _, flag := range []string{"--metrics-url", "--metrics-publish-url", "--metrics-token-file"} {
		if _, _, err := parseControllerLaunch(controllerLaunchArgs(flag, "x")); err == nil {
			t.Errorf("%s accepted: a stale installer would run without its metrics source", flag)
		}
	}
}

// The broker validates against the policy's provided sources and the
// controller binds the environment document's: one install must give both the
// same source set, or a detector the broker accepted cannot resolve live.
func TestBrokerPolicyAndControllerEnvironmentProvideTheSameSources(t *testing.T) {
	for name, config := range map[string]install.Config{
		"prometheus-default": {Namespace: "shop", BackendRecipes: telemetrysources.Recipes(), TelemetryProfile: mustBuiltin(t, "prometheus-default", "shop")},
		"none":               {Namespace: "shop", BackendRecipes: telemetrysources.Recipes(), TelemetryProfile: mustBuiltin(t, "none", "shop")},
		"external source":    {Namespace: "shop", BackendRecipes: telemetrysources.Recipes(), TelemetryProfile: withSource(t, mustBuiltin(t, "none", "shop"), "ops=http://prom.ops.svc:9090")},
	} {
		policy, err := installTelemetryPolicy(config, "shop", "shop")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var fromEnvironment []telemetry.Source
		for _, binding := range config.TelemetryEnvironment().ProvidedBindings() {
			source, err := telemetryRegistry().Source(binding)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			fromEnvironment = append(fromEnvironment, source)
		}
		if !reflect.DeepEqual(policy.Provided, fromEnvironment) {
			t.Errorf("%s: broker provides %+v, controller binds %+v", name, policy.Provided, fromEnvironment)
		}
	}
}
