package validate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/telemetry"
)

// Telemetry bindings: the static rules the detection designer's
// .sdo/diagnostics/telemetry files pass through. Every rule has a bad row; the
// policy and the real adapter registry are wired the way the CLI wires them.

func telemetryTestPolicy() telemetry.Policy {
	return telemetry.Policy{
		ControlNamespace:     "sdo-ctl",
		ApplicationNamespace: "shop",
		Credentials:          []string{"prom-token"},
		Capabilities:         []telemetry.ClusterCapability{telemetry.CapPodDiscovery, telemetry.CapNodeProxy},
		Recipes: []telemetry.Recipe{
			{Name: "prometheus", Declares: []telemetry.ClusterCapability{telemetry.CapPodDiscovery, telemetry.CapNodeProxy}},
			{Name: "plain"},
		},
		Provided: []telemetry.Source{{ID: "ops", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Adapter: "prometheus", Origin: telemetry.OriginOperator}},
	}
}

func withTelemetry(mutate func(*telemetry.Policy)) func(*Request) {
	return func(r *Request) {
		p := telemetryTestPolicy()
		if mutate != nil {
			mutate(&p)
		}
		r.Telemetry, r.TelemetryRegistry = p, telemetrysources.Registry()
	}
}

const (
	telemetryPromSource = "  - id: prom\n    kinds: [metrics]\n    adapter: prometheus\n    endpoint: http://sdo-prometheus.sdo-ctl.svc:9090\n"
	telemetryBackends   = "version: 1\nbackends:\n  - id: prom-stack\n    recipe: prometheus\n    uses: [pod-discovery, node-proxy]\n    params: {retention: 6h}\n"
)

func telemetrySources(entries ...string) string {
	return "version: 1\nsources:\n" + strings.Join(entries, "")
}

func telemetryRow(name string, files map[string]string, mutate func(*telemetry.Policy), rules ...string) gateRow {
	path := ""
	if len(rules) > 0 {
		path = layout.TelemetrySourcesFile
		if _, ok := files[layout.TelemetrySourcesFile]; !ok {
			path = layout.TelemetryBackendsFile
		}
	}
	return gateRow{name: name, kind: gateStatic, rules: rules, path: path, request: withTelemetry(mutate), edit: func(t *testing.T, r string) {
		for p, content := range files {
			write(t, r, p, content)
		}
	}}
}

func sources(entries ...string) map[string]string {
	return map[string]string{layout.TelemetrySourcesFile: telemetrySources(entries...)}
}

func both(s, b string) map[string]string {
	return map[string]string{layout.TelemetrySourcesFile: s, layout.TelemetryBackendsFile: b}
}

// withDetectorTouch lets a designer-initial row satisfy the proposal's own
// requirement to change detectors, so the row isolates the telemetry grant.
func withDetectorTouch(edits ...func(*testing.T, string)) func(*testing.T, string) {
	return func(t *testing.T, r string) {
		data, err := os.ReadFile(filepath.Join(r, gateHealthManifest))
		if err != nil {
			t.Fatal(err)
		}
		write(t, r, gateHealthManifest, string(data)+"# touched\n")
		for _, e := range edits {
			e(t, r)
		}
	}
}

var telemetryRuleRows = []gateRow{
	telemetryRow("a source with a backend, readiness and publish block passes",
		both(telemetrySources(telemetryPromSource+"    default: true\n    backend: prom-stack\n    credential: prom-token\n    ready: {text: up, min_results: 1}\n    publish:\n      scrape: {annotations: true}\n      otlp: {metrics: \"http://sdo-prometheus.sdo-ctl.svc:9090/api/v1/otlp/v1/metrics\"}\n"), telemetryBackends), nil),
	telemetryRow("a source in the application namespace passes", sources("  - id: app\n    kinds: [metrics]\n    adapter: prometheus\n    endpoint: http://prom.shop.svc.cluster.local:9090\n"), nil),

	// telemetry.schema
	telemetryRow("sources file with an unknown field", sources(telemetryPromSource+"    colour: red\n"), nil, RuleTelemetrySchema),
	telemetryRow("sources file with the wrong version", map[string]string{layout.TelemetrySourcesFile: "version: 2\nsources: []\n"}, nil, RuleTelemetrySchema),
	telemetryRow("sources file with duplicate keys", map[string]string{layout.TelemetrySourcesFile: "version: 1\nversion: 1\n"}, nil, RuleTelemetrySchema),
	telemetryRow("two sources share an id", sources(telemetryPromSource, telemetryPromSource), nil, RuleTelemetrySchema),
	telemetryRow("a source id outside the grammar", sources(strings.Replace(telemetryPromSource, "id: prom", "id: Prom_1", 1)), nil, RuleTelemetrySchema),
	telemetryRow("an unknown kind", sources(strings.Replace(telemetryPromSource, "[metrics]", "[events]", 1)), nil, RuleTelemetrySchema),
	telemetryRow("a ready probe without text", sources(telemetryPromSource+"    ready: {min_results: 1}\n"), nil, RuleTelemetrySchema),
	telemetryRow("a negative min_results", sources(telemetryPromSource+"    ready: {text: up, min_results: -1}\n"), nil, RuleTelemetrySchema),
	telemetryRow("backends file with an unknown field", map[string]string{layout.TelemetryBackendsFile: "version: 1\nbackends:\n  - id: a\n    recipe: plain\n    image: x\n"}, nil, RuleTelemetrySchema),
	telemetryRow("more sources than the cap", map[string]string{layout.TelemetrySourcesFile: "version: 1\nsources:\n" + strings.Repeat("  - id: a\n", 17)}, nil, RuleTelemetrySchema),

	// telemetry.adapter
	telemetryRow("an adapter that is not registered", sources(strings.Replace(telemetryPromSource, "adapter: prometheus", "adapter: graphite", 1)), nil, RuleTelemetryAdapter),
	telemetryRow("a kind the adapter does not serve", sources(strings.Replace(telemetryPromSource, "[metrics]", "[logs]", 1)), nil, RuleTelemetryAdapter),
	telemetryRow("no kinds", sources(strings.Replace(telemetryPromSource, "kinds: [metrics]", "kinds: []", 1)), nil, RuleTelemetryAdapter),
	telemetryRow("a kind listed twice", sources(strings.Replace(telemetryPromSource, "[metrics]", "[metrics, metrics]", 1)), nil, RuleTelemetryAdapter),

	// telemetry.endpoint
	telemetryRow("an endpoint in a namespace the environment did not grant", sources(strings.Replace(telemetryPromSource, "sdo-ctl", "kube-system", 1)), nil, RuleTelemetryEndpoint),
	telemetryRow("an endpoint that is an IP literal", sources(strings.Replace(telemetryPromSource, "sdo-prometheus.sdo-ctl.svc", "10.0.0.5", 1)), nil, RuleTelemetryEndpoint),
	telemetryRow("an endpoint on localhost", sources(strings.Replace(telemetryPromSource, "sdo-prometheus.sdo-ctl.svc", "localhost", 1)), nil, RuleTelemetryEndpoint),
	telemetryRow("an endpoint at the metadata address", sources(strings.Replace(telemetryPromSource, "sdo-prometheus.sdo-ctl.svc:9090", "169.254.169.254", 1)), nil, RuleTelemetryEndpoint),
	telemetryRow("an endpoint with userinfo", sources(strings.Replace(telemetryPromSource, "http://", "http://u:p@", 1)), nil, RuleTelemetryEndpoint),
	telemetryRow("an endpoint on a host outside the allowlist", sources(strings.Replace(telemetryPromSource, "sdo-prometheus.sdo-ctl.svc:9090", "prom.example.com", 1)), nil, RuleTelemetryEndpoint),
	telemetryRow("an endpoint on an allowlisted external host passes", sources(strings.Replace(telemetryPromSource, "sdo-prometheus.sdo-ctl.svc:9090", "prom.example.com", 1)),
		func(p *telemetry.Policy) { p.ExternalHosts = []string{"prom.example.com"} }),
	telemetryRow("a publish URL outside the granted namespaces", sources(telemetryPromSource+"    publish:\n      otlp: {metrics: \"http://evil.example.com/v1\"}\n"), nil, RuleTelemetryEndpoint),

	// telemetry.credential
	telemetryRow("a credential the environment did not grant", sources(telemetryPromSource+"    credential: other-token\n"), nil, RuleTelemetryCredential),

	// telemetry.options
	telemetryRow("options the adapter does not define", sources(telemetryPromSource+"    options: {tenant: acme}\n"), nil, RuleTelemetryOptions),

	// telemetry.default
	telemetryRow("two defaults for one kind", sources(telemetryPromSource+"    default: true\n", strings.Replace(telemetryPromSource, "id: prom", "id: prom2", 1)+"    default: true\n"), nil, RuleTelemetryDefault),
	telemetryRow("a default that collides with the operator's default", sources(telemetryPromSource+"    default: true\n"),
		func(p *telemetry.Policy) { p.Provided[0].Default = true }, RuleTelemetryDefault),

	// telemetry.backend
	telemetryRow("a backend reference that does not resolve", sources(telemetryPromSource+"    backend: nope\n"), nil, RuleTelemetryBackend),
	telemetryRow("a recipe that is not offered", map[string]string{layout.TelemetryBackendsFile: "version: 1\nbackends:\n  - id: a\n    recipe: helm\n"}, nil, RuleTelemetryBackend),
	telemetryRow("a capability the environment did not grant", map[string]string{layout.TelemetryBackendsFile: telemetryBackends},
		func(p *telemetry.Policy) { p.Capabilities = []telemetry.ClusterCapability{telemetry.CapPodDiscovery} }, RuleTelemetryBackend),
	telemetryRow("node-proxy through a recipe that does not declare it", map[string]string{layout.TelemetryBackendsFile: "version: 1\nbackends:\n  - id: a\n    recipe: plain\n    uses: [node-proxy]\n"}, nil, RuleTelemetryBackend),
	telemetryRow("an unknown capability", map[string]string{layout.TelemetryBackendsFile: "version: 1\nbackends:\n  - id: a\n    recipe: plain\n    uses: [cluster-admin]\n"}, nil, RuleTelemetryBackend),
	telemetryRow("the same backend id twice", map[string]string{layout.TelemetryBackendsFile: "version: 1\nbackends:\n  - id: a\n    recipe: plain\n  - id: a\n    recipe: plain\n"}, nil, RuleTelemetryBackend),

	// telemetry.origin
	telemetryRow("a designer file that sets an origin", sources(telemetryPromSource+"    origin: operator\n"), nil, RuleTelemetryOrigin),
	telemetryRow("a source id that shadows an operator-provided source", sources(strings.Replace(telemetryPromSource, "id: prom", "id: ops", 1)), nil, RuleTelemetryOrigin),

	// telemetry.mode
	telemetryRow("any binding file in a pinned profile", sources(telemetryPromSource), func(p *telemetry.Policy) { p.Mode = telemetry.ModePinned }, RuleTelemetryMode),
	telemetryRow("any binding file in floor mode", map[string]string{layout.TelemetryBackendsFile: telemetryBackends}, func(p *telemetry.Policy) { p.Mode = telemetry.ModeFloor }, RuleTelemetryMode),

	// A missing registry fails closed instead of skipping the adapter check.
	{name: "bindings checked without an adapter registry", kind: gateStatic, rules: []string{RuleTelemetryAdapter}, path: layout.TelemetrySourcesFile,
		request: func(r *Request) { r.Telemetry = telemetryTestPolicy() },
		edit:    gateWrite(layout.TelemetrySourcesFile, telemetrySources(telemetryPromSource))},

	// Ownership: the detection designer's initial phase only.
	{name: "designer writes the bindings", kind: gateOwnership, actor: HealthJudge, phase: InitialPhase, request: withTelemetry(nil),
		edit: withDetectorTouch(gateWrite(layout.TelemetrySourcesFile, telemetrySources(telemetryPromSource)), gateWrite(layout.TelemetryBackendsFile, telemetryBackends))},
	{name: "responder cannot write sources", kind: gateOwnership, actor: Responder, phase: ReflectionPhase, rules: []string{RuleOwnershipDenied}, path: layout.TelemetrySourcesFile, request: withTelemetry(nil),
		edit: gateWrite(layout.TelemetrySourcesFile, telemetrySources(telemetryPromSource))},
	{name: "deployer cannot write backends", kind: gateOwnership, actor: Deployer, phase: InitialPhase, rules: []string{RuleOwnershipDenied}, path: layout.TelemetryBackendsFile, request: withTelemetry(nil),
		edit: gateWrite(layout.TelemetryBackendsFile, telemetryBackends)},
	{name: "upkeep cannot write sources", kind: gateOwnership, actor: Upkeep, phase: InitialPhase, rules: []string{RuleOwnershipDenied}, path: layout.TelemetrySourcesFile, request: withTelemetry(nil),
		edit: gateWrite(layout.TelemetrySourcesFile, telemetrySources(telemetryPromSource))},
	{name: "designer outside the initial phase cannot write sources", kind: gateOwnership, actor: HealthJudge, phase: ReflectionPhase, rules: []string{RuleOwnershipDenied}, path: layout.TelemetrySourcesFile, request: withTelemetry(nil),
		edit: gateWrite(layout.TelemetrySourcesFile, telemetrySources(telemetryPromSource))},
	{name: "human cannot write sources", kind: gateOwnership, actor: Human, phase: InitialPhase, rules: []string{RuleOwnershipDenied}, path: layout.TelemetrySourcesFile, request: withTelemetry(nil),
		edit: gateWrite(layout.TelemetrySourcesFile, telemetrySources(telemetryPromSource))},
	{name: "designer cannot write bindings in a pinned profile", kind: gateOwnership, actor: HealthJudge, phase: InitialPhase, rules: []string{RuleOwnershipDenied}, path: layout.TelemetrySourcesFile,
		request: withTelemetry(func(p *telemetry.Policy) { p.Mode = telemetry.ModePinned }),
		edit:    withDetectorTouch(gateWrite(layout.TelemetrySourcesFile, telemetrySources(telemetryPromSource)))},
	{name: "designer cannot write bindings in floor mode", kind: gateOwnership, actor: HealthJudge, phase: InitialPhase, rules: []string{RuleOwnershipDenied}, path: layout.TelemetryBackendsFile,
		request: withTelemetry(func(p *telemetry.Policy) { p.Mode = telemetry.ModeFloor }),
		edit:    withDetectorTouch(gateWrite(layout.TelemetryBackendsFile, telemetryBackends))},
}

func init() { gateRows = append(gateRows, telemetryRuleRows...) }

func TestTelemetryCheckIsNotTheCompileLayer(t *testing.T) {
	// The binding rules are static: they run in the layers that never execute
	// candidate Go code, so a rejected binding blocks before compilation.
	root := goodTree(t)
	write(t, root, layout.TelemetrySourcesFile, telemetrySources(strings.Replace(telemetryPromSource, "adapter: prometheus", "adapter: graphite", 1)))
	request := Request{Root: root, Layers: Structural | Semantic}
	withTelemetry(nil)(&request)
	if got := ruleSet(diagnose(t, request)); !same(got, []string{RuleTelemetryAdapter}) {
		t.Fatalf("rules = %v", got)
	}
}
