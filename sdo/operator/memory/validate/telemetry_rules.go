package validate

import (
	"fmt"
	"slices"
	"strings"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/telemetry"
)

// telemetryRules checks the designer's telemetry bindings without executing
// anything: the files parse strictly, every source names a registered adapter
// and kinds it serves, endpoints stay inside the environment's grants, a
// credential is one the environment named, defaults are unambiguous, backends
// use only offered recipes and granted capabilities, and a pinned or floor
// profile admits no designer files. Whether detector queries resolve against
// these bindings is the compile layer's check.
func (s *run) telemetryRules() {
	sourcesFile, hasSources := s.telemetry[layout.KindTelemetrySources]
	backendsFile, hasBackends := s.telemetry[layout.KindTelemetryBackends]
	if !hasSources && !hasBackends {
		return
	}
	policy := s.req.Telemetry
	if mode := policy.EffectiveMode(); mode != telemetry.ModeOpen {
		for _, f := range []textFile{sourcesFile, backendsFile} {
			if f.path != "" {
				s.telemetryDiag(RuleTelemetryMode, f.path, fmt.Sprintf("the environment's telemetry mode is %s, which admits no designer-authored bindings", mode),
					"delete the file: the environment provides the sources, and detectors may query them by id")
			}
		}
		return
	}
	var backends []layout.TelemetryBackend
	backendIDs := map[string]bool{}
	if hasBackends {
		parsed, err := layout.ParseTelemetryBackends(backendsFile.content)
		if err != nil {
			s.telemetryDiag(RuleTelemetrySchema, backendsFile.path, err.Error(), backendsHint)
		} else {
			backends = parsed.Backends
			s.checkBackends(backendsFile.path, backends, backendIDs, policy)
		}
	}
	if hasSources {
		parsed, err := layout.ParseTelemetrySources(sourcesFile.content)
		if err != nil {
			s.telemetryDiag(RuleTelemetrySchema, sourcesFile.path, err.Error(), sourcesHint)
			return
		}
		s.checkSources(sourcesFile.path, parsed.Sources, backendIDs, policy)
	}
}

const (
	sourcesHint  = "a sources file is `version: 1` and `sources:` entries with id, kinds, adapter and endpoint; see sdo-operationalize references/telemetry.md"
	backendsHint = "a backends file is `version: 1` and `backends:` entries with id, recipe, uses and params"
)

func (s *run) telemetryDiag(rule, path, message, hint string) {
	s.add(Semantic, Diagnostic{Rule: rule, Path: path, Message: message, Hint: hint})
}

func (s *run) checkBackends(path string, backends []layout.TelemetryBackend, ids map[string]bool, policy telemetry.Policy) {
	for i, b := range backends {
		at := fmt.Sprintf("backends[%d]", i)
		if !telemetry.SourceIDPattern.MatchString(b.ID) {
			s.telemetryDiag(RuleTelemetrySchema, path, fmt.Sprintf("%s.id %q must match %s", at, b.ID, telemetry.SourceIDPattern), backendsHint)
		} else if ids[b.ID] {
			s.telemetryDiag(RuleTelemetryBackend, path, fmt.Sprintf("backend id %q is used twice", b.ID), "give each backend a unique id")
		}
		ids[b.ID] = true
		recipe, ok := policy.Recipe(b.Recipe)
		if !ok {
			var offered []string
			for _, r := range policy.Recipes {
				offered = append(offered, r.Name)
			}
			s.telemetryDiag(RuleTelemetryBackend, path, fmt.Sprintf("backend %q names recipe %q, which the environment does not offer (offered: %s)", b.ID, b.Recipe, listOrNone(offered)), "choose an offered recipe; free-form backend manifests are not supported yet")
		}
		for _, use := range b.Uses {
			c := telemetry.ClusterCapability(use)
			switch {
			case !c.Valid():
				s.telemetryDiag(RuleTelemetryBackend, path, fmt.Sprintf("backend %q uses unknown capability %q", b.ID, use), "capabilities are pod-discovery, node-proxy and pod-logs")
			case !policy.Granted(c):
				s.telemetryDiag(RuleTelemetryBackend, path, fmt.Sprintf("backend %q uses capability %q, which the environment did not grant", b.ID, use), "drop it from `uses`; capabilities are installer grants an agent cannot create")
			case ok && !slices.Contains(recipe.Declares, c):
				s.telemetryDiag(RuleTelemetryBackend, path, fmt.Sprintf("backend %q uses capability %q, which recipe %q does not declare", b.ID, use, b.Recipe), "a capability is reachable only from a recipe that declares it; node-proxy is a strong credential")
			}
		}
	}
}

func (s *run) checkSources(path string, sources []layout.TelemetrySource, backendIDs map[string]bool, policy telemetry.Policy) {
	registry := s.req.TelemetryRegistry
	seen := map[string]bool{}
	defaults := map[telemetry.Kind]string{}
	for _, src := range policy.Provided {
		seen[src.ID] = true
		if src.Default {
			for _, k := range src.Kinds {
				defaults[k] = src.ID
			}
		}
	}
	provided := map[string]bool{}
	for _, src := range policy.Provided {
		provided[src.ID] = true
	}
	for i, src := range sources {
		at := fmt.Sprintf("sources[%d]", i)
		if src.Origin != "" {
			s.telemetryDiag(RuleTelemetryOrigin, path, fmt.Sprintf("%s sets origin %q; origins are assigned by Go, never by a designer file", at, src.Origin), "delete the `origin` key")
		}
		switch {
		case !telemetry.SourceIDPattern.MatchString(src.ID):
			s.telemetryDiag(RuleTelemetrySchema, path, fmt.Sprintf("%s.id %q must match %s", at, src.ID, telemetry.SourceIDPattern), sourcesHint)
		case provided[src.ID]:
			s.telemetryDiag(RuleTelemetryOrigin, path, fmt.Sprintf("source id %q is an operator-provided source; a designer file may query it but not redefine it", src.ID), "choose another id, or delete the entry and name the provided source in detector queries")
			continue
		case seen[src.ID]:
			s.telemetryDiag(RuleTelemetrySchema, path, fmt.Sprintf("source id %q is used twice", src.ID), "give each source a unique id")
		}
		seen[src.ID] = true

		var kinds []telemetry.Kind
		kindsValid := true
		for _, k := range src.Kinds {
			if !telemetry.Kind(k).Valid() {
				kindsValid = false
				s.telemetryDiag(RuleTelemetrySchema, path, fmt.Sprintf("source %q lists unknown kind %q", src.ID, k), "kinds are metrics, logs and traces")
			}
			kinds = append(kinds, telemetry.Kind(k))
		}
		if src.Ready != nil {
			switch {
			case strings.TrimSpace(src.Ready.Text) == "":
				s.telemetryDiag(RuleTelemetrySchema, path, fmt.Sprintf("source %q ready probe needs its query text", src.ID), "write `ready: {text: \"up\", min_results: 1}`")
			case len(src.Ready.Text) > telemetry.MaxTextBytes:
				s.telemetryDiag(RuleTelemetrySchema, path, fmt.Sprintf("source %q ready text exceeds %d bytes", src.ID, telemetry.MaxTextBytes), "shorten the query")
			case src.Ready.MinResults < 0 || src.Ready.MinResults > telemetry.MaxSeries:
				s.telemetryDiag(RuleTelemetrySchema, path, fmt.Sprintf("source %q ready min_results %d is outside 0 to %d", src.ID, src.Ready.MinResults, telemetry.MaxSeries), "use 1 for a source that must answer at least one series")
			}
		}
		access := policy.CheckBinding(telemetry.Binding{Endpoint: telemetry.Endpoint{URL: src.Endpoint, Credential: src.Credential}})
		if err := access.Endpoint; err != nil {
			s.telemetryDiag(RuleTelemetryEndpoint, path, fmt.Sprintf("source %q: %v", src.ID, err), "bind a service in the control or application namespace, or a host the environment allowlisted")
		}
		if src.Publish != nil && src.Publish.OTLP != nil && src.Publish.OTLP.Metrics != "" {
			if err := policy.CheckEndpoint(src.Publish.OTLP.Metrics); err != nil {
				s.telemetryDiag(RuleTelemetryEndpoint, path, fmt.Sprintf("source %q publish otlp metrics URL: %v", src.ID, err), "applications push only to services the environment granted")
			}
		}
		if err := access.Credential; err != nil {
			s.telemetryDiag(RuleTelemetryCredential, path, fmt.Sprintf("source %q: %v", src.ID, err), "name a granted credential or leave `credential` empty; values are never written to memory")
		}
		if src.Backend != "" && !backendIDs[src.Backend] {
			s.telemetryDiag(RuleTelemetryBackend, path, fmt.Sprintf("source %q names backend %q, which backends.yaml does not define", src.ID, src.Backend), "define the backend in backends.yaml, or drop `backend:` for a backend that already exists")
		}
		if src.Default {
			for _, k := range kinds {
				if other, dup := defaults[k]; dup {
					s.telemetryDiag(RuleTelemetryDefault, path, fmt.Sprintf("source %q and %q are both the default %s source", src.ID, other, k), "at most one default per kind across operator-provided and designer sources; unset `default` on one")
				} else {
					defaults[k] = src.ID
				}
			}
		}
		s.checkAdapter(path, src, kinds, kindsValid, registry)
	}
}

// checkAdapter runs the registry's pure checks: the adapter exists, kinds are a
// non-empty subset of what it serves, and it accepts the options and endpoint.
func (s *run) checkAdapter(path string, src layout.TelemetrySource, kinds []telemetry.Kind, kindsValid bool, registry *telemetry.Registry) {
	const adapterHint = "choose a registered adapter and kinds it serves; the registry lists them in the error"
	if registry == nil {
		s.telemetryDiag(RuleTelemetryAdapter, path, fmt.Sprintf("source %q cannot be checked: no telemetry adapter registry is configured for this check", src.ID), "run the check through sdo-operator, which wires the registry")
		return
	}
	desc, ok := registry.Describe(src.Adapter)
	if !ok {
		s.telemetryDiag(RuleTelemetryAdapter, path, fmt.Sprintf("source %q names adapter %q, which is not registered (registered: %s)", src.ID, src.Adapter, strings.Join(registry.Adapters(), ", ")), adapterHint)
		return
	}
	if len(kinds) == 0 {
		s.telemetryDiag(RuleTelemetryAdapter, path, fmt.Sprintf("source %q declares no kinds (adapter %q serves %v)", src.ID, src.Adapter, desc.Kinds), adapterHint)
		return
	}
	bad := !kindsValid
	seen := map[telemetry.Kind]bool{}
	for _, k := range kinds {
		switch {
		case !k.Valid():
		case seen[k]:
			bad = true
			s.telemetryDiag(RuleTelemetryAdapter, path, fmt.Sprintf("source %q lists kind %q twice", src.ID, k), adapterHint)
		case !slices.Contains(desc.Kinds, k):
			bad = true
			s.telemetryDiag(RuleTelemetryAdapter, path, fmt.Sprintf("source %q: adapter %q does not serve kind %q (serves %v)", src.ID, src.Adapter, k, desc.Kinds), adapterHint)
		}
		seen[k] = true
	}
	if bad || !telemetry.SourceIDPattern.MatchString(src.ID) {
		return
	}
	binding, err := telemetryBinding(src, kinds)
	if err != nil {
		s.telemetryDiag(RuleTelemetryOptions, path, err.Error(), "options are plain YAML mappings, strings, numbers and lists")
		return
	}
	if err := registry.Check(binding); err != nil {
		s.telemetryDiag(RuleTelemetryOptions, path, err.Error(), fmt.Sprintf("the %s adapter decodes options strictly; remove keys it does not define", src.Adapter))
	}
}

func listOrNone(items []string) string {
	if len(items) == 0 {
		return "none"
	}
	return strings.Join(items, ", ")
}
