package layout

import (
	"strings"
	"testing"
)

func TestClassifyTelemetry(t *testing.T) {
	for p, want := range map[string]Kind{
		TelemetrySourcesFile:  KindTelemetrySources,
		TelemetryBackendsFile: KindTelemetryBackends,
	} {
		kind, id, ok := Classify(p)
		if !ok || kind != want || id != "" {
			t.Errorf("Classify(%q) = %q, %q, %v; want %q", p, kind, id, ok, want)
		}
	}
	if TelemetrySourcesFile != ".sdo/diagnostics/telemetry/sources.yaml" || TelemetryBackendsFile != ".sdo/diagnostics/telemetry/backends.yaml" {
		t.Fatalf("telemetry paths moved: %s, %s", TelemetrySourcesFile, TelemetryBackendsFile)
	}
}

func TestClassifyTelemetryIsStrict(t *testing.T) {
	for _, p := range []string{
		".sdo/diagnostics/telemetry", ".sdo/diagnostics/telemetry/", ".sdo/diagnostics/telemetry/sources.yml",
		".sdo/diagnostics/telemetry/other.yaml", ".sdo/diagnostics/telemetry/sources.yaml/x",
		".sdo/diagnostics/telemetry/sub/sources.yaml", ".sdo/telemetry/sources.yaml", ".sdo/diagnostics/sources.yaml",
	} {
		if kind, id, ok := Classify(p); ok {
			t.Errorf("Classify(%q) = %q, %q; want unclassified", p, kind, id)
		}
	}
}

const goodTelemetrySources = `version: 1
sources:
  - id: prom
    kinds: [metrics]
    adapter: prometheus
    endpoint: http://sdo-prometheus.sdo-ctl.svc:9090
    default: true
    backend: prom-stack
    credential: ""
    options: {}
    ready: {text: "up", min_results: 1}
    publish:
      scrape: {annotations: true}
      otlp: {metrics: http://sdo-prometheus.sdo-ctl.svc:9090/api/v1/otlp/v1/metrics}
`

func TestParseTelemetrySources(t *testing.T) {
	got, err := ParseTelemetrySources([]byte(goodTelemetrySources))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Sources) != 1 {
		t.Fatalf("sources = %d", len(got.Sources))
	}
	s := got.Sources[0]
	if s.ID != "prom" || s.Adapter != "prometheus" || !s.Default || s.Backend != "prom-stack" || s.Kinds[0] != "metrics" ||
		s.Ready == nil || s.Ready.Text != "up" || s.Ready.MinResults != 1 || s.Publish == nil || !s.Publish.Scrape.Annotations {
		t.Fatalf("decoded %+v", s)
	}
}

func TestParseTelemetrySourcesRejects(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"unknown field":    {"version: 1\nsources:\n  - id: a\n    kinds: [metrics]\n    adapter: p\n    endpoint: http://a.b.svc\n    surprise: 1\n", "surprise"},
		"duplicate key":    {"version: 1\nversion: 1\nsources: []\n", "duplicate"},
		"wrong version":    {"version: 2\nsources: []\n", "version"},
		"missing version":  {"sources: []\n", "version"},
		"two documents":    {"version: 1\nsources: []\n---\nversion: 1\n", "one YAML document"},
		"too many sources": {"version: 1\nsources:\n" + strings.Repeat("  - id: a\n", 17), "at most 16"},
		"too large":        {"version: 1\n# " + strings.Repeat("x", 33<<10) + "\nsources: []\n", "32 KiB"},
		"not yaml":         {"version: [", "YAML"},
		"empty":            {"", "version"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseTelemetrySources([]byte(tc.in)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestParseTelemetryBackends(t *testing.T) {
	got, err := ParseTelemetryBackends([]byte("version: 1\nbackends:\n  - id: prom-stack\n    recipe: prometheus\n    uses: [pod-discovery, node-proxy]\n    params: {retention: 6h}\n"))
	if err != nil {
		t.Fatal(err)
	}
	b := got.Backends[0]
	if b.ID != "prom-stack" || b.Recipe != "prometheus" || len(b.Uses) != 2 || b.Params["retention"] != "6h" {
		t.Fatalf("decoded %+v", b)
	}
	for name, in := range map[string]string{
		"unknown field": "version: 1\nbackends:\n  - id: a\n    recipe: r\n    image: x\n",
		"too many":      "version: 1\nbackends:\n" + strings.Repeat("  - id: a\n", 5),
		"duplicate key": "version: 1\nbackends: []\nbackends: []\n",
	} {
		if _, err := ParseTelemetryBackends([]byte(in)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
