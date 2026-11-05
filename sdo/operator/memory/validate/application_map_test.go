package validate

import (
	"context"
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
)

// A tiny generic shop: web -> api -> db (holds data), api -> cache. Nothing
// here is application specific; the map rules are generic.
const mapInventory = `---
schema_version: 1
generated_at_commit: base
application: shop
---
# Manifest inventory

- Deployment/shop/web (source: k8s/web.yaml)
- Service/shop/web (source: k8s/web.yaml)
- Deployment/shop/api (source: k8s/api.yaml)
- Service/shop/api (source: k8s/api.yaml)
- StatefulSet/shop/db (source: k8s/db.yaml)
- Service/shop/db (source: k8s/db.yaml)
- Deployment/shop/cache (source: k8s/cache.yaml)
- Service/shop/cache (source: k8s/cache.yaml)
`

const mapYAML = `version: 1
components:
  - {name: web, workload: Deployment/web, services: [web], holds_data: false}
  - {name: api, workload: Deployment/api, services: [api], holds_data: false}
  - {name: db, workload: StatefulSet/db, services: [db], holds_data: true}
  - {name: cache, workload: Deployment/cache, services: [cache], holds_data: false}
edges:
  - {from: web, to: api, evidence: [config], source: config/web.json}
  - {from: api, to: db, evidence: [config], source: config/api.json}
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

func mapRegion(yaml string) string {
	return appmap.BeginMarker + "\n```yaml\n" + yaml + "```\n" + appmap.EndMarker + "\n"
}

func mapWorkload(name string, scenarios ...string) string {
	text := "apiVersion: sdo.dev/v1alpha1\nkind: TrafficWorkload\nname: " + name + "\npurpose: health-probe\nscenarios:\n"
	for _, id := range scenarios {
		text += "  - id: " + id + "\n"
	}
	return text
}

// mapTree is a valid tree whose architecture carries the shop map; a nil
// region leaves the architecture with the inventory alone.
func mapTree(t *testing.T, regionYAML *string) string {
	root := goodTree(t)
	text := mapInventory
	if regionYAML != nil {
		text += "\n" + mapRegion(*regionYAML)
	}
	write(t, root, layout.ArchitectureFile, text)
	dir := ".sdo/diagnostics/traffic/workloads/"
	write(t, root, dir+"probe-call.yaml", mapWorkload("probe-call", "call-all"))
	write(t, root, dir+"probe-write.yaml", mapWorkload("probe-write", "write-item"))
	write(t, root, dir+"probe-read-later.yaml", mapWorkload("probe-read-later", "items-later"))
	return root
}

func staticMapCheck(t *testing.T, root string, actor Actor) (blocking, notes []Diagnostic) {
	t.Helper()
	request := Request{Root: root, Layers: Structural | Semantic, Actor: actor, Phase: InitialPhase, CoverageGaps: &notes}
	return diagnose(t, request), notes
}

func mapRules(diagnostics []Diagnostic) []string {
	var out []string
	for _, d := range diagnostics {
		if strings.HasPrefix(d.Rule, "map.") {
			out = append(out, d.Rule)
		}
	}
	return out
}

func TestApplicationMapRuleIDsMatchTheParser(t *testing.T) {
	for got, want := range map[string]string{
		RuleMapMissing: appmap.RuleMissing, RuleMapSchema: appmap.RuleSchema, RuleMapUnknownName: appmap.RuleUnknownName,
		RuleMapPathMismatch: appmap.RulePathMismatch, RuleMapUncoveredOp: appmap.RuleUncoveredOp,
		RuleMapUncoveredWrite: appmap.RuleUncoveredWrite, RuleMapUncoveredData: appmap.RuleUncoveredData,
		RuleMapUnexercisedEdge: appmap.RuleUnexercisedEdge,
	} {
		if got != want {
			t.Errorf("rule %q != %q", got, want)
		}
	}
}

func TestNoApplicationMapIsANoteSoExistingMemoryKeepsValidating(t *testing.T) {
	for _, actor := range []Actor{HealthJudge, Responder, Upkeep} {
		blocking, notes := staticMapCheck(t, mapTree(t, nil), actor)
		if len(blocking) != 0 {
			t.Fatalf("%s: a memory without a map was rejected: %v", actor, blocking)
		}
		if !slices.ContainsFunc(notes, func(d Diagnostic) bool { return d.Rule == RuleMapMissing }) {
			t.Fatalf("%s: no map.missing note: %v", actor, notes)
		}
	}
}

func TestCompleteApplicationMapPassesWithOnlyTheEdgeNote(t *testing.T) {
	yaml := mapYAML
	blocking, notes := staticMapCheck(t, mapTree(t, &yaml), HealthJudge)
	if len(blocking) != 0 {
		t.Fatalf("rejected: %v", blocking)
	}
	if got := mapRules(notes); !same(got, nil) && !(len(got) == 1 && got[0] == RuleMapUnexercisedEdge) {
		t.Fatalf("notes = %v, want only the unexercised edge api -> cache: %v", got, notes)
	}
	if !slices.ContainsFunc(notes, func(d Diagnostic) bool {
		return d.Rule == RuleMapUnexercisedEdge && strings.Contains(d.Message, "api -> cache")
	}) {
		t.Fatalf("no note naming api -> cache: %v", notes)
	}
}

func TestApplicationMapGapsBlockTheDesignerAndAreNotesForOthers(t *testing.T) {
	for _, tc := range []struct {
		name    string
		yaml    string
		rule    string
		mention string
		hint    string
	}{
		{"operation without a call probe", strings.Replace(mapYAML, "operations: [list-items, add-item]}\n  - {scenario: write", "operations: [list-items]}\n  - {scenario: write", 1), RuleMapUncoveredOp, "add-item", "call"},
		{"writing operation without a write-read probe", strings.Replace(mapYAML, "operations: [add-item, list-items]", "operations: [list-items]", 1), RuleMapUncoveredWrite, "add-item", "write-read"},
		{"data component without a read-later probe", strings.Replace(mapYAML, "path: [web, api, db]\n  - id: add-item", "path: [web, api]\n  - id: add-item", 1), RuleMapUncoveredData, "db", "read-later"},
		{"unknown component", strings.Replace(mapYAML, "to: cache", "to: queue", 1), RuleMapUnknownName, "queue", ""},
		{"workload missing from the inventory", strings.Replace(mapYAML, "StatefulSet/db", "StatefulSet/dbx", 1), RuleMapUnknownName, "StatefulSet/dbx", ""},
		{"entry service not of the entry component", strings.Replace(mapYAML, "entry: {service: web, port: 80, method: GET", "entry: {service: api, port: 80, method: GET", 1), RuleMapPathMismatch, "list-items", ""},
		{"schema: unknown field", mapYAML + "flavor: x\n", RuleMapSchema, "flavor", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := mapTree(t, &tc.yaml)
			blocking, _ := staticMapCheck(t, root, HealthJudge)
			var got Diagnostic
			for _, d := range blocking {
				if d.Rule == tc.rule {
					got = d
				}
			}
			if got.Rule == "" || !strings.Contains(got.Message, tc.mention) || !strings.Contains(got.Hint, tc.hint) || got.Path != layout.ArchitectureFile {
				t.Fatalf("designer: no blocking %s naming %q (hint %q): %v", tc.rule, tc.mention, tc.hint, blocking)
			}
			for _, actor := range []Actor{Responder, Upkeep} {
				other, notes := staticMapCheck(t, root, actor)
				if len(other) != 0 {
					t.Fatalf("%s: a designer gap rejected another actor: %v", actor, other)
				}
				if !slices.ContainsFunc(notes, func(d Diagnostic) bool { return d.Rule == tc.rule }) {
					t.Fatalf("%s: no %s note: %v", actor, tc.rule, notes)
				}
			}
		})
	}
}

func TestApplicationMapChecksWorkloadMembership(t *testing.T) {
	yaml := mapYAML
	root := mapTree(t, &yaml)
	write(t, root, ".sdo/diagnostics/traffic/workloads/probe-call.yaml", mapWorkload("probe-call", "call-all", "extra"))
	blocking, _ := staticMapCheck(t, root, HealthJudge)
	if !slices.ContainsFunc(blocking, func(d Diagnostic) bool { return d.Rule == RuleMapSchema && strings.Contains(d.Message, "extra") }) {
		t.Fatalf("a scenario with no probe entry was accepted: %v", blocking)
	}
}

func TestApplicationMapNamesNoPhysicalPaths(t *testing.T) {
	yaml := strings.Replace(mapYAML, "source: config/web.json", "source: .sdo/playbooks/x", 1)
	blocking, _ := staticMapCheck(t, mapTree(t, &yaml), HealthJudge)
	if !slices.ContainsFunc(blocking, func(d Diagnostic) bool { return d.Rule == RuleMapSchema && strings.Contains(d.Message, "physical") }) {
		t.Fatalf("physical path in the map was accepted: %v", blocking)
	}
}

func mapOwnership(t *testing.T, actor Actor, base string, candidate string) []Diagnostic {
	t.Helper()
	root := goodTree(t)
	write(t, root, layout.ArchitectureFile, base)
	commit(t, root, "base")
	write(t, root, layout.ArchitectureFile, candidate)
	if actor == HealthJudge {
		// A designer proposal must carry detectors; touch one so only arch.md ownership is under test.
		edit(t, root, gateHealth, "package", "// touched\npackage")
	}
	request := Request{Root: root, Layers: Ownership, Base: "HEAD", Actor: actor, Phase: InitialPhase}
	return diagnose(t, request)
}

func TestOnlyTheDesignerChangesTheMapRegion(t *testing.T) {
	withMap := mapInventory + "\n" + mapRegion(mapYAML)
	edited := mapInventory + "\n" + mapRegion(strings.Replace(mapYAML, "evidence: [observed]", "evidence: [config, observed]", 1))
	inventoryRewrite := strings.Replace(mapInventory, "generated_at_commit: base", "generated_at_commit: later", 1)

	for _, tc := range []struct {
		name      string
		actor     Actor
		base, new string
		want      []string
	}{
		{"designer adds the first map after the inventory", HealthJudge, mapInventory, mapInventory + "\n" + mapRegion(mapYAML), nil},
		{"designer edits the map", HealthJudge, withMap, edited, nil},
		{"designer rewrites the inventory", HealthJudge, withMap, inventoryRewrite + "\n" + mapRegion(mapYAML), []string{RuleMapOutsideRegion}},
		{"designer deletes the whole region", HealthJudge, withMap, mapInventory, []string{RuleMapOutsideRegion}},
		{"designer breaks a marker", HealthJudge, withMap, strings.Replace(withMap, appmap.EndMarker, "", 1), []string{RuleMapOutsideRegion}},
		{"upkeep carries the region verbatim", Upkeep, withMap, appmap.Carry(withMap, inventoryRewrite), nil},
		{"upkeep drops the map", Upkeep, withMap, inventoryRewrite, []string{RuleMapRegionOwner}},
		{"upkeep edits the map", Upkeep, withMap, edited, []string{RuleMapRegionOwner}},
		{"upkeep adds a map", Upkeep, mapInventory, withMap, []string{RuleMapRegionOwner}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			for _, d := range mapOwnership(t, tc.actor, tc.base, tc.new) {
				if d.Rule != RuleOwnershipDenied || d.Path != layout.ArchitectureFile {
					got = append(got, d.Rule)
				}
			}
			if !same(sortedUnique(got), tc.want) {
				t.Fatalf("rules = %v, want %v", got, tc.want)
			}
		})
	}
}

func sortedUnique(items []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, item := range items {
		if !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	slices.Sort(out)
	return out
}

func TestCheckLetsReflectionPassWithADesignerGap(t *testing.T) {
	yaml := strings.Replace(mapYAML, "operations: [add-item, list-items]", "operations: [list-items]", 1)
	root := mapTree(t, &yaml)
	commit(t, root, "base")
	write(t, root, ".sdo/playbooks/base/README.md", gateReadmeText("More detail.\n"))
	generateIndex(t, root)
	var notes []Diagnostic
	request := Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase, CoverageGaps: &notes, Decision: gateUpdated("base"),
		Publication: &PublicationContext{BaseCommit: "HEAD", IncidentID: gateIncident}}
	diagnostics, err := Check(context.Background(), request, gateStubCompile)
	for _, d := range diagnostics {
		if strings.HasPrefix(d.Rule, "map.") {
			t.Fatalf("a designer gap rejected reflection: %v (%v)", d, err)
		}
	}
	if !slices.ContainsFunc(notes, func(d Diagnostic) bool { return d.Rule == RuleMapUncoveredWrite }) {
		t.Fatalf("reflection did not see the gap as a note: %v", notes)
	}
}
