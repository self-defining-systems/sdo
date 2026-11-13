package runtime

import (
	"context"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

const mapTestDoc = "# Architecture\n\n- possible edge: mailer -> api (Deployment/mailer Secret smtp)\n\n" + `<!-- sdo:application-map:begin -->
` + "```yaml" + `
version: 1
components:
  - {name: front, workload: Deployment/front, services: [front], holds_data: false}
  - {name: api, workload: Deployment/api, services: [api], holds_data: false}
  - {name: db, workload: StatefulSet/db, services: [db], holds_data: true, data: {engine: postgres, role: authoritative}}
  - {name: cache, workload: Deployment/cache, services: [cache], holds_data: true, data: {engine: redis, role: derived, derived_from: [db]}}
  - {name: jobs, workload: Deployment/jobs, services: [jobs], holds_data: false}
  - {name: mailer, workload: Deployment/mailer, services: [mailer], holds_data: false}
edges:
  - {from: front, to: api, evidence: [config], source: x}
  - {from: api, to: db, evidence: [config], source: x}
  - {from: api, to: cache, evidence: [config], source: x}
  - {from: jobs, to: db, evidence: [config], source: x}
operations:
  - {id: list, writes: false, entry: {service: front, port: 80, method: GET, path: /items}, path: [front, api, cache]}
  - {id: create, writes: true, entry: {service: front, port: 80, method: POST, path: /items}, path: [front, api, db]}
probes:
  - {scenario: list, kind: call, operations: [list]}
  - {scenario: create, kind: write-read, operations: [create]}
` + "```" + `
<!-- sdo:application-map:end -->
`

// mapView is a read view that also serves the accepted architecture text.
type mapView struct {
	program.ReadView
	doc string
	err error
}

func (v mapView) ArchitectureText() (string, error) { return v.doc, v.err }

func mapLoop(doc string, err error, failing ...string) *ResponderLoop {
	view := detectApp(failing...)
	loop := NewResponderLoop(detectRequest(), nil, staticProvider{view.Snapshot}, nil, PlanExecutor{}, 10)
	loop.Now = detectNow
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return mapView{ReadView: program.ReadFrom(snapshot), doc: doc, err: err}
	}
	return loop
}

func mapCall(t *testing.T, loop *ResponderLoop, view, name string) ToolResponse {
	t.Helper()
	return loop.Handle(context.Background(), ToolRequest{Command: "map", MapView: view, Name: name})
}

func wantAll(t *testing.T, text string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(text, want) {
			t.Fatalf("output lacks %q:\n%s", want, text)
		}
	}
}

func TestMapListsComponentsOperationsAndEdgeCount(t *testing.T) {
	r := mapCall(t, mapLoop(mapTestDoc, nil, "create"), "", "")
	if r.Error != "" {
		t.Fatal(r.Error)
	}
	wantAll(t, r.Text, "components (6)", "db  StatefulSet/db  services: db  holds data: postgres authoritative",
		"front  Deployment/front  services: front  no data", "operations (2)", "create  write", "list  read", "edges: 4 (+1 possible, unconfirmed)")
	if !concurrentRead("map") {
		t.Fatal("map is a concurrent read")
	}
}

func TestMapPathShowsOrderDataProbesAndStatus(t *testing.T) {
	r := mapCall(t, mapLoop(mapTestDoc, nil, "create"), "path", "create")
	if r.Error != "" {
		t.Fatal(r.Error)
	}
	wantAll(t, r.Text, "operation create (write) enters front:80 POST /items", "path: front -> api -> db [data: postgres authoritative]",
		"edges: front->api, api->db", "probe create (write-read): FAIL")
	r = mapCall(t, mapLoop(mapTestDoc, nil, "create"), "path", "list")
	wantAll(t, r.Text, "cache [data: redis derived from db]", "probe list (call): PASS")
}

func TestMapNearShowsBothDirectionsPossibleEdgesAndOperations(t *testing.T) {
	loop := mapLoop(mapTestDoc, nil, "create")
	r := mapCall(t, loop, "near", "api")
	wantAll(t, r.Text, "upstream (it depends on, 2 hops): db [data: postgres authoritative], cache [data: redis derived from db]",
		"downstream (depend on it, 2 hops): front, mailer (possible, unconfirmed)", "operations through it: list, create")
	r = mapCall(t, loop, "near", "db")
	wantAll(t, r.Text, "upstream (it depends on, 2 hops): none", "downstream (depend on it, 2 hops): api, jobs, front (via api), mailer (via api) (possible, unconfirmed)")
}

func TestMapUnknownNamesListTheValidOnes(t *testing.T) {
	loop := mapLoop(mapTestDoc, nil, "create")
	r := mapCall(t, loop, "path", "nope")
	wantAll(t, r.Error, `unknown operation "nope"`, "create, list")
	r = mapCall(t, loop, "near", "nope")
	wantAll(t, r.Error, `unknown component "nope"`, "api, cache, db, front, jobs, mailer")
	r = mapCall(t, loop, "path", "")
	wantAll(t, r.Error, "needs a name")
}

func TestMapWithoutAnApplicationMapSaysSo(t *testing.T) {
	for name, loop := range map[string]*ResponderLoop{
		"no region":      mapLoop("# Architecture\n", nil, "create"),
		"unreadable doc": mapLoop("", context.DeadlineExceeded, "create"),
	} {
		r := mapCall(t, loop, "", "")
		if !strings.Contains(r.Error, "no application map") {
			t.Fatalf("%s: error = %q", name, r.Error)
		}
	}
	view := detectApp()
	bare := NewResponderLoop(detectRequest(), nil, staticProvider{view.Snapshot}, nil, PlanExecutor{}, 10)
	if r := mapCall(t, bare, "", ""); !strings.Contains(r.Error, "no application map") {
		t.Fatalf("error = %q", r.Error)
	}
}
