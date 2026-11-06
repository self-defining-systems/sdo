package validate

import (
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/telemetry"
)

func ids(sources []telemetry.Source) []string {
	out := []string{}
	for _, s := range sources {
		out = append(out, s.ID)
	}
	return out
}

func TestEffectiveTelemetrySourcesByMode(t *testing.T) {
	root := t.TempDir()
	write(t, root, layout.TelemetrySourcesFile, telemetrySources(telemetryPromSource))
	policy := telemetryTestPolicy()
	registry := telemetrysources.Registry()
	for _, tc := range []struct {
		name string
		mode telemetry.Mode
		want []string
	}{
		{"open: provided then designer", telemetry.ModeOpen, []string{"ops", "prom"}},
		{"unset mode is open", "", []string{"ops", "prom"}},
		{"pinned: provided only", telemetry.ModePinned, []string{"ops"}},
		{"floor: nothing", telemetry.ModeFloor, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy.Mode = tc.mode
			got, err := EffectiveTelemetrySources(root, policy, registry)
			if err != nil {
				t.Fatal(err)
			}
			if got == nil || !slices.Equal(ids(got), tc.want) {
				t.Fatalf("sources = %v (nil=%v), want %v", ids(got), got == nil, tc.want)
			}
		})
	}
}

func TestEffectiveTelemetrySourcesCarryAdapterCapabilities(t *testing.T) {
	root := t.TempDir()
	write(t, root, layout.TelemetrySourcesFile, telemetrySources(telemetryPromSource))
	got, err := EffectiveTelemetrySources(root, telemetry.Policy{}, telemetrysources.Registry())
	if err != nil || len(got) != 1 {
		t.Fatalf("sources = %+v, %v", got, err)
	}
	if !HasAlertSource(got) || got[0].Origin == "" {
		t.Fatalf("the prometheus adapter serves alerts and every source has an origin: %+v", got[0])
	}
	if HasAlertSource([]telemetry.Source{{ID: "ops", Kinds: []telemetry.Kind{telemetry.KindMetrics}}}) {
		t.Fatal("a source without the alerts capability is not an alert source")
	}
}

func TestEffectiveTelemetrySourcesFailClosed(t *testing.T) {
	root := t.TempDir()
	write(t, root, layout.TelemetrySourcesFile, telemetrySources(strings.Replace(telemetryPromSource, "adapter: prometheus", "adapter: graphite", 1)))
	if _, err := EffectiveTelemetrySources(root, telemetry.Policy{}, telemetrysources.Registry()); err == nil {
		t.Fatal("an unregistered adapter must fail, not be skipped")
	}
	if _, err := EffectiveTelemetrySources(root, telemetry.Policy{}, nil); err == nil {
		t.Fatal("bindings with no registry must fail")
	}
	empty := t.TempDir()
	if got, err := EffectiveTelemetrySources(empty, telemetry.Policy{}, nil); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("no file, no sources: %v, %v", got, err)
	}
}
