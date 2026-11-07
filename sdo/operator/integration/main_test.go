//go:build integration

package integration

import (
	"os"
	"testing"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/telemetry"
)

// TestMain runs this test binary as the broker's validation subprocess when
// invoked as one, exactly as the operator's main does. The registry is the
// operator's plus a metrics adapter that serves no alerts.
func TestMain(m *testing.M) {
	broker.RunValidationChild(testRegistry())
	os.Exit(m.Run())
}

func testRegistry() *telemetry.Registry {
	registry := telemetrysources.Registry()
	err := telemetry.Register(registry, telemetry.Adapter[struct{}]{
		Name: "plain-metrics", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Dialect: "synthetic metrics", Caps: []telemetry.Capability{telemetry.CapRange},
		Validate: func(struct{}, telemetry.Endpoint) error { return nil },
		Open:     func(struct{}, telemetry.Endpoint, telemetry.Env) (telemetry.Client, error) { return nil, nil },
	})
	if err != nil {
		panic(err)
	}
	return registry
}

func mustBuiltin(t *testing.T, name, control string) telemetry.Profile {
	t.Helper()
	profile, ok := telemetrysources.BuiltinProfile(name, control)
	if !ok {
		t.Fatalf("no built-in profile %q", name)
	}
	return profile
}
