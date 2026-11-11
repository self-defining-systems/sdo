package appmap

import (
	"strings"
	"testing"
)

// A map built without application source marks what the rendered manifests
// (manifest) or the live application (observed) showed, as opposed to what
// the source or its config files declare (config).
func TestProvenanceAcceptsAManifestDerivedMap(t *testing.T) {
	text := strings.Replace(shopYAML, "evidence: [config], source: config/api.json", "evidence: [manifest], source: Deployment/api env DB_HOST=db:5432", 1)
	text = strings.Replace(text, "  - {name: db, workload: StatefulSet/db, services: [db, db-headless], holds_data: true}",
		"  - {name: db, workload: StatefulSet/db, services: [db, db-headless], holds_data: true, evidence: [manifest], data: {engine: postgres, role: authoritative}}", 1)
	text = strings.Replace(text, "from: api, to: db, evidence: [config, observed], source: config/api.json", "from: api, to: db, evidence: [manifest, observed], source: Deployment/api env DB_HOST=db:5432", 1)
	m := mustParse(t, text)
	for _, f := range m.Check(ParseInventory(shopInventory), shopWorkloads) {
		if f.Rule != RuleUnexercisedEdge {
			t.Fatalf("finding = %+v", f)
		}
	}
	for _, comp := range m.Components {
		if comp.Name == "db" && (len(comp.Evidence) != 1 || comp.Evidence[0] != EvidenceManifest) {
			t.Fatalf("db evidence = %v", comp.Evidence)
		}
	}
}

func TestProvenanceRejectsUnknownEvidence(t *testing.T) {
	text := strings.Replace(shopYAML, "  - {name: db, workload: StatefulSet/db, services: [db, db-headless], holds_data: true}",
		"  - {name: db, workload: StatefulSet/db, services: [db, db-headless], holds_data: true, evidence: [guess]}", 1)
	text = strings.Replace(text, "evidence: [observed], source: seen in api logs", "evidence: [hunch], source: seen in api logs", 1)
	var rules []string
	for _, f := range mustParse(t, text).Check(ParseInventory(shopInventory), shopWorkloads) {
		rules = append(rules, f.Rule+": "+f.Message)
	}
	joined := strings.Join(rules, "\n")
	if !strings.Contains(joined, `component db: evidence "guess"`) || !strings.Contains(joined, `evidence "hunch"`) {
		t.Fatalf("findings = %s", joined)
	}
}
