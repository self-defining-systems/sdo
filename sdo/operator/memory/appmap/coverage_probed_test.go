package appmap

import "testing"

// A synthetic app entered at front. The probed operation runs front > api > db;
// api is configured to use a registry (a mapped edge, on no operation path),
// which keeps its data in registry-db. side is a request hop of an operation
// no probe exercises; front also calls it. cart > cart-db is the path of a
// probe its workload declares knownBroken.
const probedYAML = `version: 1
components:
  - {name: front, workload: Deployment/front, services: [front], holds_data: false}
  - {name: api, workload: Deployment/api, services: [api], holds_data: false}
  - {name: db, workload: StatefulSet/db, services: [db], holds_data: true}
  - {name: registry, workload: StatefulSet/registry, services: [registry], holds_data: false}
  - {name: registry-db, workload: StatefulSet/registry-db, services: [registry-db], holds_data: true}
  - {name: side, workload: Deployment/side, services: [side], holds_data: false}
  - {name: cart, workload: Deployment/cart, services: [cart], holds_data: false}
  - {name: cart-db, workload: StatefulSet/cart-db, services: [cart-db], holds_data: true}
  - {name: idle, workload: StatefulSet/idle, services: [idle], holds_data: true}
edges:
  - {from: front, to: api, evidence: [config], source: x}
  - {from: api, to: db, evidence: [config], source: x}
  - {from: api, to: registry, evidence: [config], source: x}
  - {from: registry, to: registry-db, evidence: [config], source: x}
  - {from: front, to: side, evidence: [config], source: x}
  - {from: front, to: cart, evidence: [config], source: x}
  - {from: cart, to: cart-db, evidence: [config], source: x}
operations:
  - {id: get, writes: false, entry: {service: front, port: 80, method: GET, path: /}, path: [front, api, db]}
  - {id: side, writes: false, entry: {service: front, port: 80, method: GET, path: /side}, path: [front, side]}
  - {id: cart, writes: false, entry: {service: front, port: 80, method: GET, path: /cart}, path: [front, cart, cart-db]}
probes:
  - {scenario: call-get, kind: call, operations: [get]}
  - {scenario: call-cart, kind: call, operations: [cart]}
`

const probedInventory = `- Service/demo/front (source: k.yaml)
  - exposure:NodePort
`

// Probed is about the user path a probe exercises: a component a probed
// component depends on without being a request hop (a registry, discovery or
// config service, and what it depends on) counts, and so does a path whose
// probe is knownBroken (a broken path is when its stores matter most). A
// request hop no probe exercises does not, and neither does a component no
// probed component reaches. The designer's coverage obligation is unchanged.
func TestProbedCountsDependenciesAndKnownBrokenPaths(t *testing.T) {
	m := mustParse(t, probedYAML)
	workloads := []Workload{{Name: "probe-call", Scenarios: []string{"call-get", "call-cart"}, KnownBroken: []string{"call-cart"}}}
	cov := m.Coverage(ParseInventory(probedInventory), workloads)
	want := map[string]bool{"front": true, "api": true, "db": true, "registry": true, "registry-db": true, "cart": true, "cart-db": true, "side": false, "idle": false}
	for name, probed := range want {
		if got := cov.Components[name].Probed(); got != probed {
			t.Errorf("%s: Probed() = %v, want %v (%+v)", name, got, probed, cov.Components[name])
		}
	}
	if c := cov.Components["cart-db"]; len(c.Probes) != 0 || len(c.BrokenProbes) != 1 || !c.Obligation() {
		t.Errorf("cart-db: a knownBroken probe neither covers it nor clears its obligation: %+v", c)
	}
	if c := cov.Components["registry"]; len(c.Probes) != 0 || !c.DependedOn {
		t.Errorf("registry: %+v, want depended on without a covering probe", c)
	}
}
