package appmap

import (
	"strings"
	"testing"
)

// A tiny generic shop: web -> api -> db (holds data), api -> cache.
const shopYAML = `version: 1
components:
  - {name: web, workload: Deployment/web, services: [web], holds_data: false}
  - {name: api, workload: Deployment/api, services: [api], holds_data: false}
  - {name: db, workload: StatefulSet/db, services: [db, db-headless], holds_data: true}
  - {name: cache, workload: Deployment/cache, services: [cache], holds_data: false}
edges:
  - {from: web, to: api, evidence: [config], source: config/web.json}
  - {from: api, to: db, evidence: [config, observed], source: config/api.json}
  - {from: api, to: cache, evidence: [observed], source: seen in api logs}
operations:
  - id: list-items
    writes: false
    entry: {service: web, port: 80, method: GET, path: /items}
    path: [web, api, db]
  - id: add-item
    writes: true
    entry: {service: web, port: 80, method: POST, path: /items}
    path: [web, api, db]
probes:
  - {scenario: call-all, kind: call, operations: [list-items, add-item]}
  - {scenario: write-item, kind: write-read, operations: [add-item, list-items]}
  - {scenario: items-later, kind: read-later, operations: [list-items]}
`

const shopInventory = `---
schema_version: 1
---
# Manifest inventory

- Deployment/shop/web (source: k8s/web.yaml)
- Service/shop/web (source: k8s/web.yaml)
- Deployment/shop/api (source: k8s/api.yaml)
- Service/shop/api (source: k8s/api.yaml)
- StatefulSet/shop/db (source: k8s/db.yaml)
- Service/shop/db (source: k8s/db.yaml)
- Service/shop/db-headless (source: k8s/db.yaml)
- Deployment/shop/cache (source: k8s/cache.yaml)
- Service/shop/cache (source: k8s/cache.yaml)
`

var shopWorkloads = []Workload{
	{Name: "probe-call", Scenarios: []string{"call-all"}},
	{Name: "probe-write", Scenarios: []string{"write-item"}},
	{Name: "probe-read-later", Scenarios: []string{"items-later"}},
}

func mustParse(t *testing.T, text string) Map {
	t.Helper()
	m, err := Parse(text)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func rules(findings []Finding) []string {
	var out []string
	for _, f := range findings {
		out = append(out, f.Rule)
	}
	return out
}

func has(findings []Finding, rule, mention string) bool {
	for _, f := range findings {
		if f.Rule == rule && strings.Contains(f.Message, mention) {
			return true
		}
	}
	return false
}

func TestFindLocatesTheRegion(t *testing.T) {
	doc := shopInventory + "\n" + BeginMarker + "\n```yaml\n" + shopYAML + "```\n" + EndMarker + "\n"
	region, found, err := Find(doc)
	if err != nil || !found {
		t.Fatalf("Find = %v %v", found, err)
	}
	if region.YAML != shopYAML {
		t.Fatalf("yaml = %q", region.YAML)
	}
	if line := strings.Split(doc, "\n")[region.Line-1]; line != BeginMarker {
		t.Fatalf("Line %d points at %q", region.Line, line)
	}
	if line := strings.Split(doc, "\n")[region.YAMLLine-1]; line != "version: 1" {
		t.Fatalf("YAMLLine %d points at %q", region.YAMLLine, line)
	}
	if got := Outside(doc); got != strings.TrimRight(shopInventory, "\n") {
		t.Fatalf("Outside = %q", got)
	}
}

func TestFindRejectsMalformedRegions(t *testing.T) {
	fence := "```yaml\nversion: 1\n```\n"
	for name, doc := range map[string]string{
		"begin without end": "x\n" + BeginMarker + "\n" + fence,
		"end without begin": "x\n" + fence + EndMarker + "\n",
		"two regions":       BeginMarker + "\n" + fence + EndMarker + "\n" + BeginMarker + "\n" + fence + EndMarker + "\n",
		"no fence":          BeginMarker + "\nversion: 1\n" + EndMarker + "\n",
		"second fence":      BeginMarker + "\n" + fence + "```\n" + EndMarker + "\n",
	} {
		if _, found, err := Find(doc); !found || err == nil {
			t.Errorf("%s: found=%v err=%v, want an error", name, found, err)
		}
	}
	if _, found, err := Find("# plain\n"); found || err != nil {
		t.Errorf("no markers: found=%v err=%v", found, err)
	}
}

func TestCarryKeepsTheRegionVerbatim(t *testing.T) {
	previous := "old inventory\n\n" + BeginMarker + "\n```yaml\nversion: 1\n# keep me\n```\n" + EndMarker + "\n"
	got := Carry(previous, "new inventory\n")
	if !strings.Contains(got, "# keep me") || Outside(got) != "new inventory" {
		t.Fatalf("Carry = %q", got)
	}
	if again := Carry(got, "new inventory\n"); again != got {
		t.Fatalf("carrying twice changed the document:\n%q\n%q", got, again)
	}
	if got := Carry("no map\n", "new\n"); got != "new\n" {
		t.Fatalf("Carry without a region = %q", got)
	}
}

func TestShopMapIsClean(t *testing.T) {
	m := mustParse(t, shopYAML)
	// The only finding is the note about the edge no operation path crosses.
	if got := m.Check(ParseInventory(shopInventory), shopWorkloads); len(got) != 1 || !has(got, RuleUnexercisedEdge, "api -> cache") {
		t.Fatalf("findings: %+v", got)
	}
	scenarios := []Scenario{
		{ID: "call-all", SideEffect: "read", DependsOn: []string{"web", "api", "db"}},
		{ID: "write-item", SideEffect: "write-with-cleanup", DependsOn: []string{"web", "api", "db-headless"}},
		{ID: "items-later", SideEffect: "read", DependsOn: []string{"web", "api", "db"}, SetupSteps: 1},
	}
	if got := m.CheckScenarios(scenarios); len(got) != 0 {
		t.Fatalf("scenario findings: %+v", got)
	}
}

func TestParseRejectsUnknownFieldsAndDuplicateKeys(t *testing.T) {
	if _, err := Parse(shopYAML + "extra: 1\n"); err == nil {
		t.Error("unknown field accepted")
	}
	if _, err := Parse("version: 1\nversion: 1\n"); err == nil {
		t.Error("duplicate key accepted")
	}
}

func replace(from, to string) string { return strings.Replace(shopYAML, from, to, 1) }

func TestCheckNamesWhatIsWrong(t *testing.T) {
	for _, tc := range []struct {
		name    string
		yaml    string
		rule    string
		mention string
	}{
		{"operation in no call probe", replace("operations: [list-items, add-item]}\n  - {scenario: write", "operations: [list-items]}\n  - {scenario: write"), RuleUncoveredOp, "add-item"},
		{"writing operation in no write-read probe", replace("operations: [add-item, list-items]", "operations: [list-items]"), RuleUncoveredWrite, "add-item"},
		{"data component on no read-later path", replace("path: [web, api, db]\n  - id: add-item", "path: [web, api]\n  - id: add-item"), RuleUncoveredData, "db"},
		{"unknown component in an edge", replace("to: cache", "to: queue"), RuleUnknownName, "queue"},
		{"unknown operation in a probe", replace("operations: [list-items]}\n", "operations: [list-itemz]}\n"), RuleUnknownName, "list-itemz"},
		{"workload not in the inventory", replace("Deployment/cache", "Deployment/cachee"), RuleUnknownName, "Deployment/cachee"},
		{"service not in the inventory", replace("db-headless", "db-missing"), RuleUnknownName, "db-missing"},
		{"entry service not a service of the entry component", replace("entry: {service: web, port: 80, method: GET", "entry: {service: api, port: 80, method: GET"), RulePathMismatch, "list-items"},
		{"component unreachable inside the path", replace("path: [web, api, db]\n  - id: add-item", "path: [web, db]\n  - id: add-item"), RulePathMismatch, "list-items"},
		{"self edge", replace("from: web, to: api", "from: web, to: web"), RuleSchema, "self-edge"},
		{"duplicate edge", replace("{from: api, to: cache,", "{from: web, to: api,"), RuleSchema, "web -> api"},
		{"edge without evidence", replace("evidence: [observed]", "evidence: []"), RuleSchema, "api -> cache"},
		{"bad method", replace("method: GET", "method: FETCH"), RuleSchema, "FETCH"},
		{"duplicate component", replace("- {name: cache,", "- {name: web,"), RuleSchema, "web"},
		{"wrong version", replace("version: 1", "version: 2"), RuleSchema, "version"},
		{"write-read probe with no writing operation", replace("operations: [add-item, list-items]", "operations: [list-items]"), RuleSchema, "write-item"},
		{"read-later probe reaching no data", replace("kind: read-later, operations: [list-items]", "kind: read-later, operations: []"), RuleSchema, "items-later"},
		{"probe kind and workload disagree", replace("kind: call", "kind: read-later"), RuleSchema, "call-all"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := Parse(tc.yaml)
			if err != nil {
				t.Fatal(err)
			}
			got := m.Check(ParseInventory(shopInventory), shopWorkloads)
			if !has(got, tc.rule, tc.mention) {
				t.Fatalf("no %s finding mentioning %q: %+v", tc.rule, tc.mention, got)
			}
			for _, f := range got {
				if f.Hint == "" {
					t.Errorf("finding without a hint: %+v", f)
				}
			}
		})
	}
}

func TestUncoveredWriteNamesTheOperationAndTheProbeKind(t *testing.T) {
	m := mustParse(t, replace("{scenario: write-item, kind: write-read, operations: [add-item, list-items]}", "{scenario: write-item, kind: call, operations: [list-items]}"))
	got := m.Check(ParseInventory(shopInventory), []Workload{
		{Name: "probe-call", Scenarios: []string{"call-all", "write-item"}},
		{Name: "probe-read-later", Scenarios: []string{"items-later"}},
	})
	for _, f := range got {
		if f.Rule == RuleUncoveredWrite {
			if !strings.Contains(f.Message, "add-item") || !strings.Contains(f.Hint, "write-read") || !strings.Contains(f.Hint, "probe-write") {
				t.Fatalf("finding does not say which probe is missing: %+v", f)
			}
			return
		}
	}
	t.Fatalf("no %s: %+v", RuleUncoveredWrite, got)
}

func TestScenariosInWrongWorkloadOrWithoutProbe(t *testing.T) {
	m := mustParse(t, shopYAML)
	got := m.Check(ParseInventory(shopInventory), []Workload{
		{Name: "probe-call", Scenarios: []string{"call-all", "stray"}},
		{Name: "probe-write", Scenarios: []string{"write-item", "items-later"}},
		{Name: "probe-read-later", Scenarios: []string{"items-later"}},
	})
	if !has(got, RuleSchema, "stray") || !has(got, RuleSchema, "items-later") {
		t.Fatalf("findings: %+v", got)
	}
	if got := m.Check(ParseInventory(shopInventory), nil); !has(got, RuleSchema, "write-item") {
		t.Fatalf("probes with no workloads: %+v", got)
	}
}

func TestUnexercisedEdgeIsANote(t *testing.T) {
	m := mustParse(t, shopYAML)
	got := m.Check(ParseInventory(shopInventory), shopWorkloads)
	if !has(got, RuleUnexercisedEdge, "api -> cache") {
		t.Fatalf("no unexercised-edge finding: %+v", got)
	}
}

func TestCheckScenariosComparesTheCatalogWithTheMap(t *testing.T) {
	m := mustParse(t, shopYAML)
	good := []Scenario{
		{ID: "call-all", SideEffect: "read", DependsOn: []string{"web", "api", "db"}},
		{ID: "write-item", SideEffect: "idempotent-write", DependsOn: []string{"web", "api", "db"}},
		{ID: "items-later", SideEffect: "read", DependsOn: []string{"web", "api", "db"}, SetupSteps: 1},
	}
	with := func(index int, edit func(*Scenario)) []Scenario {
		out := append([]Scenario(nil), good...)
		edit(&out[index])
		return out
	}
	for _, tc := range []struct {
		name      string
		scenarios []Scenario
		rule      string
		mention   string
	}{
		{"DependsOn lacks a component's service", with(0, func(s *Scenario) { s.DependsOn = []string{"web", "api"} }), RulePathMismatch, "component db"},
		{"DependsOn names a service off the path", with(0, func(s *Scenario) { s.DependsOn = append(s.DependsOn, "cache") }), RulePathMismatch, `"cache"`},
		{"write-read scenario that only reads", with(1, func(s *Scenario) { s.SideEffect = "read" }), RuleSchema, "write-item"},
		{"read-later scenario that writes", with(2, func(s *Scenario) { s.SideEffect = "idempotent-write" }), RuleSchema, "items-later"},
		{"probe without a scenario", good[:2], RuleSchema, "items-later"},
		{"read-later scenario reading data nothing creates", with(2, func(s *Scenario) { s.SetupSteps = 0 }), RuleSchema, "setup"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := m.CheckScenarios(tc.scenarios)
			if !has(got, tc.rule, tc.mention) {
				t.Fatalf("no %s finding mentioning %q: %+v (rules %v)", tc.rule, tc.mention, got, rules(got))
			}
		})
	}
	if got := m.CheckScenarios(with(0, func(s *Scenario) { s.SideEffect = "write-with-cleanup" })); len(got) != 0 {
		t.Fatalf("a call probe may have any side effect: %+v", got)
	}
}
