// Package telemetrycontext builds the task-facing TelemetryContext message from
// the operator environment and the accepted revision's telemetry bindings. Go
// builds it; agents only read it.
package telemetrycontext

import (
	"fmt"
	"slices"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/telemetry"
)

// Build returns the telemetry in force for the accepted revision whose
// sources file is designerSources: the mode, every effective source
// (operator-provided, plus the designer's while the mode is open) with the
// dialect from its adapter descriptor, and the granted cluster capabilities.
// A nil registry leaves dialect and capabilities empty.
func Build(registry *telemetry.Registry, environment telemetry.Environment, designerSources []byte) (*contracts.TelemetryContext, error) {
	t := struct {
		Registry    *telemetry.Registry
		Environment telemetry.Environment
	}{registry, environment}
	mode := t.Environment.EffectiveMode()
	out := &contracts.TelemetryContext{Mode: string(mode)}
	for _, c := range t.Environment.Capabilities {
		out.Capabilities = append(out.Capabilities, string(c))
	}
	for _, r := range t.Environment.Recipes {
		recipe := &contracts.TelemetryRecipe{Name: r.Name}
		for _, c := range r.Declares {
			recipe.Declares = append(recipe.Declares, string(c))
		}
		out.Recipes = append(out.Recipes, recipe)
	}
	out.GrantedNamespaces = grantedNamespaces(t.Environment)
	out.GrantedHosts = slices.Clone(t.Environment.ExternalHosts)
	out.GrantedCredentials = slices.Clone(t.Environment.Credentials)
	bindings := t.Environment.ProvidedBindings()
	publish := map[string]*contracts.TelemetryPublish{}
	if base := t.Environment.MetricsPublishURL; base != "" {
		for _, b := range bindings {
			if b.Default && containsKind(b, telemetry.KindMetrics) {
				publish[b.ID] = &contracts.TelemetryPublish{ScrapeAnnotations: true,
					OtlpMetricsUrl: strings.TrimRight(base, "/") + "/api/v1/otlp/v1/metrics"}
			}
		}
	}
	if mode == telemetry.ModeOpen {
		designer, err := layout.TelemetryBindings(designerSources)
		if err != nil {
			return nil, fmt.Errorf("accepted telemetry sources: %w", err)
		}
		bindings = append(bindings, designer...)
		if len(designerSources) > 0 {
			parsed, err := layout.ParseTelemetrySources(designerSources)
			if err != nil {
				return nil, fmt.Errorf("accepted telemetry sources: %w", err)
			}
			for _, source := range parsed.Sources {
				if p := publishBlock(source.Publish); p != nil {
					publish[source.ID] = p
				}
			}
		}
	}
	for _, b := range bindings {
		source := &contracts.TelemetrySource{Id: b.ID, Adapter: b.Adapter, Default: b.Default, Endpoint: b.Endpoint.URL,
			Origin: string(b.Origin), Publish: publish[b.ID]}
		for _, k := range b.Kinds {
			source.Kinds = append(source.Kinds, string(k))
		}
		if t.Registry != nil {
			described, err := t.Registry.Source(b)
			if err != nil {
				return nil, fmt.Errorf("telemetry source %q: %w", b.ID, err)
			}
			source.Dialect = described.Dialect
			for _, c := range described.Capabilities {
				source.Capabilities = append(source.Capabilities, string(c))
			}
			source.Origin = string(described.Origin)
		}
		out.Sources = append(out.Sources, source)
	}
	return out, nil
}

func containsKind(b telemetry.Binding, kind telemetry.Kind) bool {
	for _, k := range b.Kinds {
		if k == kind {
			return true
		}
	}
	return false
}

func publishBlock(p *layout.TelemetryPublish) *contracts.TelemetryPublish {
	if p == nil {
		return nil
	}
	out := &contracts.TelemetryPublish{}
	if p.Scrape != nil {
		out.ScrapeAnnotations = p.Scrape.Annotations
	}
	if p.OTLP != nil {
		out.OtlpMetricsUrl = p.OTLP.Metrics
	}
	if !out.ScrapeAnnotations && out.OtlpMetricsUrl == "" {
		return nil
	}
	return out
}

// grantedNamespaces is the control and application namespaces plus the extras,
// in that order and without repeats: the namespaces a designer endpoint may name.
func grantedNamespaces(e telemetry.Environment) []string {
	var out []string
	for _, ns := range append([]string{e.ApplicationNamespace, e.ControlNamespace}, e.ExtraNamespaces...) {
		if ns != "" && !slices.Contains(out, ns) {
			out = append(out, ns)
		}
	}
	return out
}
