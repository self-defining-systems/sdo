package appmap

import (
	"strings"
	"testing"
)

// shopWithEntryEdges adds two edges the shop's probes never cross: the entry
// component web calling the cache directly, and an exporter (a Service no
// component calls) reading the database.
func shopWithEntryEdges(t *testing.T, edit func(string) string) (Map, Inventory) {
	t.Helper()
	text := strings.Replace(shopYAML, "edges:\n", "  - {name: exporter, workload: Deployment/exporter, services: [exporter], holds_data: false}\nedges:\n  - {from: web, to: cache, evidence: [manifest], source: ConfigMap/web key cache-url}\n  - {from: exporter, to: db, evidence: [manifest], source: ConfigMap/exporter key dsn}\n", 1)
	if edit != nil {
		text = edit(text)
	}
	inventory := shopInventory + "- Deployment/shop/exporter (source: k8s/exporter.yaml)\n- Service/shop/exporter (source: k8s/exporter.yaml)\n"
	return mustParse(t, text), ParseInventory(inventory)
}

func TestEntryEdgeOnNoProbedPathIsUncovered(t *testing.T) {
	m, inv := shopWithEntryEdges(t, nil)
	got := m.Check(inv, shopWorkloads)
	for _, edge := range []string{"web -> cache", "exporter -> db"} {
		if !has(got, RuleUncoveredEdge, edge) {
			t.Errorf("no %s finding for %s: %+v", RuleUncoveredEdge, edge, got)
		}
	}
	// api is not an entry component (web calls it): its unexercised edge stays a note.
	if has(got, RuleUncoveredEdge, "api -> cache") || !has(got, RuleUnexercisedEdge, "api -> cache") {
		t.Errorf("api -> cache should stay an unexercised-edge note: %+v", got)
	}
	for _, f := range got {
		if f.Rule == RuleUncoveredEdge && (!strings.Contains(f.Hint, "probe") || !strings.Contains(f.Hint, "unprobed")) {
			t.Errorf("hint does not say how to cover it or record why not: %+v", f)
		}
	}
	if edges := m.UncoveredEdges(); len(edges) != 2 || edges[0].From != "exporter" || edges[1].From != "web" {
		t.Errorf("UncoveredEdges = %+v", edges)
	}
}

func TestEntryEdgeCrossedByAProbedOperationIsCovered(t *testing.T) {
	m, inv := shopWithEntryEdges(t, func(text string) string {
		return strings.Replace(text, "path: [web, api, db]\n  - id: add-item", "path: [web, api, db, cache]\n  - id: add-item", 1)
	})
	got := m.Check(inv, shopWorkloads)
	if has(got, RuleUncoveredEdge, "web -> cache") {
		t.Fatalf("a probed operation crosses web -> cache: %+v", got)
	}
}

func TestUnprobedEdgeWithAReasonIsANote(t *testing.T) {
	m, inv := shopWithEntryEdges(t, func(text string) string {
		return strings.Replace(text, "source: ConfigMap/exporter key dsn}", "source: ConfigMap/exporter key dsn, unprobed: metrics scraper; no user request reaches it}", 1)
	})
	got := m.Check(inv, shopWorkloads)
	if has(got, RuleUncoveredEdge, "exporter -> db") {
		t.Fatalf("a recorded reason should replace the probe: %+v", got)
	}
	if !has(got, RuleUnprobedEdge, "exporter -> db") || !has(got, RuleUnprobedEdge, "metrics scraper") {
		t.Fatalf("no %s note carrying the reason: %+v", RuleUnprobedEdge, got)
	}
	if edges := m.UncoveredEdges(); len(edges) != 1 || edges[0].From != "web" {
		t.Errorf("UncoveredEdges = %+v", edges)
	}
}

func TestUnprobedReasonOnAProbedEdgeIsWrong(t *testing.T) {
	m := mustParse(t, replace("source: config/web.json}", "source: config/web.json, unprobed: needs a login}"))
	if got := m.Check(ParseInventory(shopInventory), shopWorkloads); !has(got, RuleSchema, "web -> api") {
		t.Fatalf("an edge a probe crosses cannot be recorded unprobed: %+v", got)
	}
}
