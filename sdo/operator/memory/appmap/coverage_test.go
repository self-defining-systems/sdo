package appmap

import (
	"strings"
	"testing"
)

// A synthetic app entered through a NodePort Service front. front calls api,
// api calls db; db calls front back (an internal callback, so front is
// "called"). metrics-exporter scrapes db and nothing calls it; ssh-gate is a
// ClusterIP Service nothing calls. The only probe scrapes /metrics.
const exposedYAML = `version: 1
components:
  - {name: front, workload: Deployment/front, services: [front], holds_data: false}
  - {name: api, workload: Deployment/api, services: [api], holds_data: false}
  - {name: db, workload: StatefulSet/db, services: [db], holds_data: true}
  - {name: metrics-exporter, workload: Deployment/metrics-exporter, services: [metrics-exporter], holds_data: false}
  - {name: ssh-gate, workload: Deployment/ssh-gate, services: [ssh-gate], holds_data: false}
edges:
  - {from: front, to: api, evidence: [config], source: config/front.json}
  - {from: api, to: db, evidence: [config], source: config/api.json}
  - {from: db, to: front, evidence: [config], source: config/db-callback.json}
  - {from: metrics-exporter, to: db, evidence: [manifest], source: ConfigMap/metrics-exporter key dsn}
  - {from: ssh-gate, to: api, evidence: [manifest], source: ConfigMap/ssh-gate key api-url}
operations:
  - id: scrape
    writes: false
    entry: {service: metrics-exporter, port: 9100, method: GET, path: /metrics}
    path: [metrics-exporter, db]
probes:
  - {scenario: scrape-db, kind: call, operations: [scrape]}
`

const exposedInventory = `# Manifest inventory

- Deployment/demo/front (source: k8s/front.yaml)
- Service/demo/front (source: k8s/front.yaml)
  - exposure:NodePort
  - selector:app=front
- Deployment/demo/api (source: k8s/api.yaml)
- Service/demo/api (source: k8s/api.yaml)
- StatefulSet/demo/db (source: k8s/db.yaml)
- Service/demo/db (source: k8s/db.yaml)
- Deployment/demo/metrics-exporter (source: k8s/exporter.yaml)
- Service/demo/metrics-exporter (source: k8s/exporter.yaml)
- Deployment/demo/ssh-gate (source: k8s/ssh.yaml)
- Service/demo/ssh-gate (source: k8s/ssh.yaml)
`

var scrapeWorkloads = []Workload{{Name: "probe-call", Scenarios: []string{"scrape-db"}}}

func coverageMessages(findings []Finding) []string {
	var out []string
	for _, f := range findings {
		if f.Rule == RuleCoverage {
			out = append(out, f.Message)
		}
	}
	return out
}

func TestInventoryReadsServiceExposure(t *testing.T) {
	inv := ParseInventory(exposedInventory)
	if got := inv.Exposure["front"]; len(got) != 1 || got[0] != "NodePort" {
		t.Fatalf("front exposure = %v", got)
	}
	if len(inv.Exposure["api"]) != 0 {
		t.Fatalf("api is not exposed: %v", inv.Exposure)
	}
}

// (a) Only an uncalled metrics exporter and an uncalled SSH Service: neither
// is exposed, so neither is an entry, and nothing asks for their probes.
func TestUncalledServicesAreNotEntries(t *testing.T) {
	m := mustParse(t, exposedYAML)
	cov := m.Coverage(ParseInventory(exposedInventory), scrapeWorkloads)
	for _, name := range []string{"metrics-exporter", "ssh-gate"} {
		if cov.Components[name].Entry || cov.Components[name].OnUserPath {
			t.Errorf("%s: %+v, want neither an entry nor on the user path", name, cov.Components[name])
		}
	}
	for _, msg := range coverageMessages(m.Check(ParseInventory(exposedInventory), scrapeWorkloads)) {
		if strings.Contains(msg, "metrics-exporter") || strings.Contains(msg, "ssh-gate") {
			t.Errorf("obligation for an uncalled, unexposed Service: %s", msg)
		}
	}
}

// (b) The exposed front is an entry even though db calls it back, and a
// metrics scrape that enters elsewhere covers nothing: front, api and db are
// all obligations.
func TestExposedEntryIsCoveredOnlyByOperationsEnteringThere(t *testing.T) {
	m := mustParse(t, exposedYAML)
	inv := ParseInventory(exposedInventory)
	cov := m.Coverage(inv, scrapeWorkloads)
	if !cov.Components["front"].Entry {
		t.Fatalf("front is exposed but not an entry: %+v", cov.Components["front"])
	}
	if len(cov.Components["db"].Probes) != 0 {
		t.Fatalf("a scrape entering at a non-entry Service covers db: %+v", cov.Components["db"])
	}
	got := coverageMessages(m.Check(inv, scrapeWorkloads))
	for _, name := range []string{"front", "api", "db"} {
		found := false
		for _, msg := range got {
			found = found || strings.HasPrefix(msg, "component "+name+" ")
		}
		if !found {
			t.Errorf("no %s obligation for %s: %v", RuleCoverage, name, got)
		}
	}
	if len(got) != 3 {
		t.Errorf("want exactly front, api and db: %v", got)
	}
}

func TestUserOperationThroughTheEntryCoversItsPath(t *testing.T) {
	text := strings.Replace(exposedYAML, "probes:\n", "  - id: browse\n    writes: false\n    entry: {service: front, port: 80, method: GET, path: /items}\n    path: [front, api, db]\nprobes:\n  - {scenario: browse-items, kind: call, operations: [browse]}\n", 1)
	m := mustParse(t, text)
	workloads := []Workload{{Name: "probe-call", Scenarios: []string{"scrape-db", "browse-items"}}}
	inv := ParseInventory(exposedInventory)
	cov := m.Coverage(inv, workloads)
	if got := cov.Components["db"].Probes; len(got) != 1 || got[0] != "browse-items" {
		t.Fatalf("db covered by %v, want only the user operation's probe", got)
	}
	if got := coverageMessages(m.Check(inv, workloads)); len(got) != 0 {
		t.Fatalf("a covered user path has no obligation: %v", got)
	}
}

// A knownBroken probe covers nothing: it fails at baseline, so it cannot
// verify recovery.
func TestKnownBrokenProbeCoversNothing(t *testing.T) {
	text := strings.Replace(exposedYAML, "probes:\n", "  - id: browse\n    writes: false\n    entry: {service: front, port: 80, method: GET, path: /items}\n    path: [front, api, db]\nprobes:\n  - {scenario: browse-items, kind: call, operations: [browse]}\n", 1)
	m := mustParse(t, text)
	workloads := []Workload{{Name: "probe-call", Scenarios: []string{"scrape-db", "browse-items"}, KnownBroken: []string{"browse-items"}}}
	if got := m.Coverage(ParseInventory(exposedInventory), workloads).Components["api"].Probes; len(got) != 0 {
		t.Fatalf("a knownBroken probe covers api: %v", got)
	}
}

// (d) A component-level unprobed reason satisfies the rule; an edge-level
// note on an internal callback does not excuse the entry.
func TestComponentUnprobedSatisfiesTheRuleAndAnEdgeNoteDoesNot(t *testing.T) {
	inv := ParseInventory(exposedInventory)
	edgeOnly := strings.Replace(exposedYAML, "source: config/db-callback.json}", "source: config/db-callback.json, unprobed: internal callback}", 1)
	got := coverageMessages(mustParse(t, edgeOnly).Check(inv, scrapeWorkloads))
	if len(got) != 3 {
		t.Fatalf("an edge note excused a component: %v", got)
	}
	escaped := edgeOnly
	for _, name := range []string{"front", "api", "db"} {
		escaped = strings.Replace(escaped, "{name: "+name+", ", "{name: "+name+", unprobed: needs credentials, ", 1)
	}
	m := mustParse(t, escaped)
	findings := m.Check(inv, scrapeWorkloads)
	if got := coverageMessages(findings); len(got) != 0 {
		t.Fatalf("component unprobed reasons should satisfy the rule: %v", got)
	}
	if !has(findings, RuleUnprobed, "needs credentials") {
		t.Fatalf("no note carrying the recorded reason: %+v", findings)
	}
	if cov := m.Coverage(inv, scrapeWorkloads); cov.Components["api"].Unprobed != "needs credentials" {
		t.Fatalf("coverage does not carry the reason: %+v", cov.Components["api"])
	}
}

func TestUnprobedOnACoveredComponentIsWrong(t *testing.T) {
	text := strings.Replace(exposedYAML, "probes:\n", "  - id: browse\n    writes: false\n    entry: {service: front, port: 80, method: GET, path: /items}\n    path: [front, api, db]\nprobes:\n  - {scenario: browse-items, kind: call, operations: [browse]}\n", 1)
	text = strings.Replace(text, "{name: api, ", "{name: api, unprobed: needs a login, ", 1)
	workloads := []Workload{{Name: "probe-call", Scenarios: []string{"scrape-db", "browse-items"}}}
	if got := mustParse(t, text).Check(ParseInventory(exposedInventory), workloads); !has(got, RuleSchema, "api") {
		t.Fatalf("a covered component cannot be recorded unprobed: %+v", got)
	}
}

// An exposed observability UI is an entry; the designer converges by saying
// why no user operation enters there.
func TestExposedObservabilityUIConvergesWithAComponentEscape(t *testing.T) {
	text := strings.Replace(exposedYAML, "edges:\n", "  - {name: trace-ui, workload: Deployment/trace-ui, services: [trace-ui], holds_data: false}\nedges:\n", 1)
	inv := ParseInventory(exposedInventory + "- Deployment/demo/trace-ui (source: k8s/ui.yaml)\n- Service/demo/trace-ui (source: k8s/ui.yaml)\n  - exposure:NodePort\n")
	if got := coverageMessages(mustParse(t, text).Check(inv, scrapeWorkloads)); len(got) != 4 {
		t.Fatalf("want front, api, db and trace-ui: %v", got)
	}
	text = strings.Replace(text, "{name: trace-ui, ", "{name: trace-ui, unprobed: operator trace viewer; no user operation, ", 1)
	if got := coverageMessages(mustParse(t, text).Check(inv, scrapeWorkloads)); len(got) != 3 {
		t.Fatalf("the escaped UI should leave only front, api and db: %v", got)
	}
}

// With nothing exposed, the entries are the entry Services of the declared
// operations, so a ClusterIP-only app keeps the obligations it can see; a
// metrics port is never one.
func TestNoExposureFallsBackToDeclaredOperationsMinusMetricsPorts(t *testing.T) {
	shop := mustParse(t, shopYAML)
	cov := shop.Coverage(ParseInventory(shopInventory), shopWorkloads)
	if !cov.Components["web"].Entry || cov.Components["api"].Entry || cov.ExposedEntries {
		t.Fatalf("fallback entries: %+v", cov)
	}
	if !cov.Components["cache"].OnUserPath || len(cov.Components["cache"].Probes) != 0 {
		t.Fatalf("cache is reachable from web and uncovered: %+v", cov.Components["cache"])
	}
	scrape := mustParse(t, exposedYAML)
	inv := ParseInventory(strings.Replace(exposedInventory, "  - exposure:NodePort\n", "", 1) + "")
	inv.MetricsPorts = map[string][]int{"metrics-exporter": {9100}}
	if cov := scrape.Coverage(inv, scrapeWorkloads); cov.Components["metrics-exporter"].Entry {
		t.Fatalf("a metrics port is an entry: %+v", cov.Components["metrics-exporter"])
	}
}

// A possible edge (only a Secret names the callee) from a user-path component
// puts its target on the user path, so the designer confirms and covers it or
// says why not.
func TestPossibleEdgeFromTheUserPathExtendsIt(t *testing.T) {
	text := strings.Replace(exposedYAML, "edges:\n", "  - {name: blobs, workload: StatefulSet/blobs, services: [blobs], holds_data: true}\nedges:\n", 1)
	doc := exposedInventory + "- StatefulSet/demo/blobs (source: k8s/blobs.yaml)\n- Service/demo/blobs (source: k8s/blobs.yaml)\n" + PossibleEdgePrefix + "api -> blobs (Secret api-env key BLOB_URL)\n"
	cov := mustParse(t, text).Coverage(ParseInventory(doc), scrapeWorkloads)
	if !cov.Components["blobs"].OnUserPath {
		t.Fatalf("a possible edge from api should put blobs on the user path: %+v", cov.Components["blobs"])
	}
}
