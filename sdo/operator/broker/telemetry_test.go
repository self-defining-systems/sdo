package broker

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/prometheus"
)

func prometheusRegistry(t *testing.T) *telemetry.Registry {
	t.Helper()
	registry := telemetry.NewRegistry()
	if err := prometheus.Register(registry); err != nil {
		t.Fatal(err)
	}
	return registry
}

// A broker publishes trees that may hold telemetry bindings, so one without
// the adapter registry cannot be built: it would otherwise reject every
// binding at publication ("no adapter registry is configured").
func TestNewRefusesABrokerWithoutTelemetryRegistry(t *testing.T) {
	_, err := New(t.TempDir(), t.TempDir(), nil, ObservationPolicy{})
	if err == nil || !strings.Contains(err.Error(), "telemetry adapter registry") {
		t.Fatalf("New without a telemetry registry = %v, want a construction error naming it", err)
	}
	_, err = New(t.TempDir(), t.TempDir(), nil, ObservationPolicy{Telemetry: Telemetry{Registry: telemetry.NewRegistry(), Policy: telemetry.Policy{Mode: "bogus"}}})
	if err == nil || !strings.Contains(err.Error(), "telemetry policy") {
		t.Fatalf("New with an invalid telemetry policy = %v, want a construction error", err)
	}
}

// The validation subprocess resolves detector queries against the same
// policy as the broker's own check: the parent hands it the check's policy and
// the child pairs it with its registry. Before, the child ran with no policy
// and no registry, so a provided source was invisible to it (the Go-installed
// alerts detector was "unbound") and any designer binding failed.
func TestValidationChildRunsUnderTheParentChecksTelemetryPolicy(t *testing.T) {
	policy := telemetry.Policy{Mode: telemetry.ModeOpen, ControlNamespace: "sdo-ctl", ApplicationNamespace: "shop",
		Provided: []telemetry.Source{{ID: "prom", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Adapter: "prometheus", Default: true, Capabilities: []telemetry.Capability{telemetry.CapAlerts}, Origin: telemetry.OriginOperator}}}
	ctx := context.WithValue(context.Background(), telemetryPolicyKey{}, policy)
	encoded, err := childTelemetryArgument(ctx)
	if err != nil {
		t.Fatal(err)
	}
	registry := telemetry.NewRegistry()
	request, err := validationChildRequest([]string{"bin", "validation-child", "/candidate", "/operator", "", "off", encoded}, registry)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(request.Telemetry, policy) || request.TelemetryRegistry != registry {
		t.Fatalf("child request telemetry = %+v (registry %p), want %+v (registry %p)", request.Telemetry, request.TelemetryRegistry, policy, registry)
	}
	if request.Root != "/candidate" || request.OperatorRoot != "/operator" || request.Replay != "off" {
		t.Fatalf("child request = %+v", request)
	}
	if _, err := childTelemetryArgument(context.Background()); err == nil {
		t.Fatal("a compile request without the check's telemetry policy must not start the child")
	}
	if _, err := validationChildRequest([]string{"bin", "validation-child", "/c", "/o", "", "", encoded}, nil); err == nil {
		t.Fatal("a validation child without a registry must refuse to run")
	}
	if _, err := validationChildRequest([]string{"bin", "validation-child", "/c", "/o", "", ""}, registry); err == nil {
		t.Fatal("an old-arity child invocation must be refused, not run without a policy")
	}
}

// The onboarding broker and every controller broker derive their telemetry
// from the environment document; operator-provided sources carry the
// capabilities the registry describes, so the alerts detector Go installs and
// the sources detector queries resolve against are the same set.
func TestTelemetryFromEnvironmentDescribesProvidedSources(t *testing.T) {
	registry := prometheusRegistry(t)
	env := telemetry.Environment{Version: telemetry.EnvironmentVersion, Capabilities: []telemetry.ClusterCapability{telemetry.CapPodDiscovery},
		ApplicationNamespace: "shop", ControlNamespace: "sdo-ctl", Recipes: []telemetry.Recipe{{Name: "prometheus"}},
		Provided: []telemetry.Binding{{ID: "prom", Adapter: "prometheus", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Default: true, Endpoint: telemetry.Endpoint{URL: "http://sdo-prometheus.sdo-ctl.svc:9090"}}}}
	got, err := TelemetryFromEnvironment(env, registry)
	if err != nil {
		t.Fatal(err)
	}
	if got.Registry != registry || got.Policy.EffectiveMode() != telemetry.ModeOpen || got.Policy.ApplicationNamespace != "shop" || got.Policy.ControlNamespace != "sdo-ctl" {
		t.Fatalf("telemetry = %+v", got)
	}
	if len(got.Policy.Provided) != 1 || got.Policy.Provided[0].ID != "prom" || got.Policy.Provided[0].Origin != telemetry.OriginOperator ||
		!slices.Contains(got.Policy.Provided[0].Capabilities, telemetry.CapAlerts) {
		t.Fatalf("provided = %+v, want prom described by the registry with origin operator", got.Policy.Provided)
	}
	if len(got.Policy.Recipes) != 1 || !slices.Equal(got.Policy.Capabilities, env.Capabilities) {
		t.Fatalf("policy = %+v", got.Policy)
	}
	floor := env
	floor.Mode = telemetry.ModeFloor
	if got, err := TelemetryFromEnvironment(floor, registry); err != nil || len(got.Policy.Provided) != 0 {
		t.Fatalf("floor = %+v, %v; want no provided source", got.Policy.Provided, err)
	}
	unknown := env
	unknown.Provided = []telemetry.Binding{{ID: "x", Adapter: "nope", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Endpoint: telemetry.Endpoint{URL: "http://x.sdo-ctl.svc"}}}
	if _, err := TelemetryFromEnvironment(unknown, registry); err == nil {
		t.Fatal("a provided source the registry cannot describe must fail the broker's construction")
	}
	if _, err := TelemetryFromEnvironment(env, nil); err == nil {
		t.Fatal("no registry must fail")
	}
}

// A controller broker reads its launch's environment document, so it applies
// the same provided sources as the onboarding broker; a launch without one is
// an open environment that provides nothing.
func TestControllerTelemetryReadsTheLaunchEnvironment(t *testing.T) {
	registry := prometheusRegistry(t)
	env := telemetry.Environment{Version: telemetry.EnvironmentVersion, ApplicationNamespace: "shop", ControlNamespace: "sdo-ctl", Recipes: []telemetry.Recipe{{Name: "prometheus"}},
		Provided: []telemetry.Binding{{ID: "prom", Adapter: "prometheus", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Default: true, Endpoint: telemetry.Endpoint{URL: "http://sdo-prometheus.sdo-ctl.svc:9090"}}}}
	document, err := env.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "environment.json")
	if err := os.WriteFile(path, document, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ControllerTelemetry(&contracts.ControllerConfig{Namespace: "shop", ControlNamespace: "sdo-ctl", TelemetryEnvironmentFile: path}, registry)
	if err != nil {
		t.Fatal(err)
	}
	want, err := TelemetryFromEnvironment(env, registry)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("controller telemetry = %+v, %v; want %+v", got, err, want)
	}
	none, err := ControllerTelemetry(&contracts.ControllerConfig{Namespace: "shop"}, registry)
	if err != nil || len(none.Policy.Provided) != 0 || none.Policy.ControlNamespace != "shop" {
		t.Fatalf("no environment = %+v, %v", none, err)
	}
}
