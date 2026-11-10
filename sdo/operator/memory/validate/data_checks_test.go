package validate

import (
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
)

// The shop map with data roles: db is the authoritative postgres store and
// cache a redis copy derived from it.
var dataMapYAML = strings.NewReplacer(
	"{name: db, workload: StatefulSet/db, services: [db], holds_data: true}",
	"{name: db, workload: StatefulSet/db, services: [db], holds_data: true, data: {engine: postgres, role: authoritative}}",
	"{name: cache, workload: Deployment/cache, services: [cache], holds_data: false}",
	"{name: cache, workload: Deployment/cache, services: [cache], holds_data: false, data: {engine: redis, role: derived, derived_from: [db]}}",
).Replace(mapYAML)

var plainMapYAML = mapYAML

const cacheEquivalence = `component: cache
engine: redis
target: {labels: {app: cache}}
read: {pattern: "item:{id}", type: hash, first: 8}
min_records: 0
lookup:
  component: db
  engine: postgres
  target: {labels: {app: db}}
  read: {table: items, key: id}
assert:
  - equivalence: [{field: price, source: price}]
`

func dataCheckDiagnostics(t *testing.T, mapYAML *string, checks map[string]string) []Diagnostic {
	t.Helper()
	root := mapTree(t, mapYAML)
	for id, body := range checks {
		write(t, root, layout.DataCheckPath(id), body)
	}
	blocking, _ := staticMapCheck(t, root, HealthJudge)
	var out []Diagnostic
	for _, d := range blocking {
		if strings.HasPrefix(d.Rule, "data-check.") {
			out = append(out, d)
		}
	}
	return out
}

func TestDataChecksResolveToMapComponentsWithDataRoles(t *testing.T) {
	if got := dataCheckDiagnostics(t, &dataMapYAML, map[string]string{"cache-prices": cacheEquivalence}); len(got) != 0 {
		t.Fatalf("a valid data check was rejected: %v", got)
	}
	for name, tc := range map[string]struct {
		mapYAML *string
		check   string
		want    string
	}{
		"unknown component":    {&dataMapYAML, strings.Replace(cacheEquivalence, "component: cache\n", "component: search\n", 1), "checked component search is not in the application map"},
		"no data role":         {&plainMapYAML, cacheEquivalence, "has no data entry"},
		"wrong engine":         {&dataMapYAML, strings.Replace(cacheEquivalence, "engine: postgres\n  target: {labels: {app: db}}\n  read: {table: items, key: id}", "engine: mongodb\n  target: {labels: {app: db}}\n  read: {database: shop, collection: items, key: id}", 1), "is a postgres store in the application map, not mongodb"},
		"equivalence not copy": {&dataMapYAML, strings.NewReplacer("component: cache\nengine: redis", "component: db\nengine: postgres", "component: db\n  engine: postgres", "component: cache\n  engine: redis", `read: {pattern: "item:{id}", type: hash, first: 8}`, `read: {table: items, key: id, first: 8}`, "read: {table: items, key: id}", `read: {pattern: "item:{id}", type: hash}`).Replace(cacheEquivalence), "must be role derived with cache in its derived_from"},
		"no map":               {nil, cacheEquivalence, "arch.md has no application map"},
	} {
		t.Run(name, func(t *testing.T) {
			got := dataCheckDiagnostics(t, tc.mapYAML, map[string]string{"cache-prices": tc.check})
			if len(got) == 0 || got[0].Rule != RuleDataCheckComponent || !strings.Contains(got[0].Message, tc.want) {
				t.Fatalf("diagnostics = %v, want %q", got, tc.want)
			}
		})
	}
}

func TestDataChecksAreValidatedAsReadOnlyAndBounded(t *testing.T) {
	writes := strings.Replace(cacheEquivalence, "read: {table: items, key: id}", `read: {table: items, key: id, where: "id = 1; DELETE FROM items"}`, 1)
	got := dataCheckDiagnostics(t, &dataMapYAML, map[string]string{"cache-prices": writes})
	if len(got) == 0 || got[0].Rule != RuleDataCheckInvalid || got[0].Path != layout.DataCheckPath("cache-prices") {
		t.Fatalf("diagnostics = %v", got)
	}
	unbounded := strings.Replace(cacheEquivalence, "first: 8", "first: 500", 1)
	if got := dataCheckDiagnostics(t, &dataMapYAML, map[string]string{"cache-prices": unbounded}); len(got) == 0 || got[0].Rule != RuleDataCheckInvalid {
		t.Fatalf("an unbounded sample was accepted: %v", got)
	}
}

// Only the detection designer changes data checks.
func TestDataChecksBelongToTheDetectionDesigner(t *testing.T) {
	if !allowed(HealthJudge, InitialPhase, layout.KindDataCheck, "cache-prices") {
		t.Fatal("the detection designer cannot write a data check")
	}
	for _, actor := range []Actor{Responder, Deployer, Upkeep, Upstream} {
		for _, phase := range []Phase{InitialPhase, ReflectionPhase, ProposalPhase, AdvancePhase} {
			if allowed(actor, phase, layout.KindDataCheck, "cache-prices") {
				t.Fatalf("%s may write a data check in the %s phase", actor, phase)
			}
		}
	}
	if kind, id, ok := layout.Classify(".sdo/diagnostics/data/cache-prices.yaml"); !ok || kind != layout.KindDataCheck || id != "cache-prices" {
		t.Fatalf("Classify = %s %s %v", kind, id, ok)
	}
	for _, p := range []string{".sdo/diagnostics/data/Cache.yaml", ".sdo/diagnostics/data/x.yml", ".sdo/diagnostics/data/sub/x.yaml", ".sdo/diagnostics/data"} {
		if _, _, ok := layout.Classify(p); ok {
			t.Fatalf("Classify(%q) classified", p)
		}
	}
}
