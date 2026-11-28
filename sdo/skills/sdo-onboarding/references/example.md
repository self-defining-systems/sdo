# Worked example

A small order service: `web` (port 80, Deployment), `orders-db` (StatefulSet, postgres), `worker` (no Service). Users
use bearer tokens to place and read orders. A worker writes receipts and an admin CLI provisions ordinary users.

## Application map

In the seeded map, keep the component and edge entries. Add the operation and its probe, using the goal's journey ID.
This fragment maps the create, detail, private list and public list operations. Add the journey IDs from the goal.
Evidence for edges read from source is `config`.

```yaml
operations:
  - id: place-order
    journey: place-and-track-order
    correct: The order and its receipt can be read back with the submitted values.
    writes: true
    entry: {service: web, port: 80, method: POST, path: /api/orders}
    path: [web, orders-db, worker]
  - id: read-receipt
    journey: place-and-track-order
    correct: The receipt names the order.
    entry: {service: web, port: 80, method: GET, path: '/api/orders/{id}'}
    path: [web, orders-db]
  - id: list-own-orders
    journey: list-orders
    correct: The user's own orders show their references and items.
    entry: {service: web, port: 80, method: GET, path: /api/orders}
    path: [web, orders-db]
  - id: browse-orders
    journey: list-orders
    correct: The default page shows public orders with valid references.
    entry: {service: web, port: 80, method: GET, path: /api/orders/public}
    path: [web, orders-db]
probes:
  - {scenario: write-order, kind: write-read, operations: [place-order, read-receipt, list-own-orders]}
  - {scenario: call-orders, kind: call, operations: [list-own-orders, browse-orders]}
  - {scenario: later-order, kind: read-later, operations: [list-own-orders]}
procedures:
  - id: probe-user
    component: web
    container: app
    command: [order-admin, users, ensure, "--name={{username}}", --password-stdin]
    params: [{name: username, type: string, required: true}]
    effect: data
    probe_identity: true
    returns_token: true
    source: docs/admin.md
```

The procedure reads one password line from stdin, ensures an ordinary account and prints one API token line on stdout.
Return a valid token for the same account after restart. `Token: "token"` binds that credential to `{token}` before any
request. For a password-only API, use `As: "pw"` and a login in Setup. Send the marked username and `{pw}` in its body,
then use `SaveJSON("token", "token")` or `SaveCookie("session", "session")` to pass the credential to later steps.

## `.sdo/diagnostics/traffic/generators/generators.go`

```go
package generators

import (
	"encoding/json"
	"strings"

	"sdo.dev/operator/sdk/traffic"
)

const marker = "sdoprobe" // in every user and object name; at least 6 characters

var (
	web  = traffic.Target{Service: "web", Port: 80}
	path = []string{"web", "orders-db"} // Services of exactly the components on the operations' map paths

	buyer  = traffic.ProbeUser{Name: "sdoprobe-buyer", Token: "token"}   // writes
	reader = traffic.ProbeUser{Name: "sdoprobe-reader", Token: "token"}  // owns the fixtures reads check
	keeper = traffic.ProbeUser{Name: "sdoprobe-keeper", Token: "token"}  // owns the read-later fixture
)

func step(name string, e traffic.Endpoint) traffic.Step { return traffic.Step{Name: name, Endpoint: e} }

// auth expands the provisioned token from State.
func auth(e *traffic.HTTPEndpoint) *traffic.HTTPEndpoint {
	return e.WithHeader("Authorization", "Bearer {token}")
}

// Setup seeds a fixed reference. Restarting the prober may find it already there.
func fixture(u traffic.ProbeUser) []traffic.Step {
	return []traffic.Step{
		step("ensure-fixture", auth(traffic.POST("/api/orders", nil).
			WithJSON(`{"ref":"sdoprobe-fixture-`+u.Name+`","item":"pen"}`, nil)).OrExisting(409, "exists")),
	}
}

// receipt waits for background work: a pending answer is sent again until the iteration times out.
type receipt struct{ *traffic.HTTPEndpoint }

func (r receipt) Check(resp traffic.Response, state traffic.State) traffic.Verdict {
	if v := r.HTTPEndpoint.Check(resp, state); !v.OK {
		return v
	}
	var body struct{ Receipt string }
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		return traffic.Fail("order is not JSON: %v", err)
	}
	if !strings.Contains(body.Receipt, state["ref"]) {
		return traffic.NotYet("no receipt for %s yet", state["ref"])
	}
	return traffic.Pass()
}

func Scenarios() traffic.Catalog {
	byRef := traffic.Params{"ref": traffic.FromPrevious("ref")}
	return traffic.Catalog{
		{
			ID: "write-order", Description: "place an order, read it back, its receipt appears", Target: web,
			DependsOn: path, Marker: marker, // SideEffect is inferred: a write with Cleanup
			Users: []traffic.ProbeUser{buyer},
			Steps: []traffic.Step{
				step("create", auth(traffic.POST("/api/orders", nil).WithJSON(`{"ref":"{ref}","item":"pen"}`,
					traffic.Params{"ref": traffic.Unique("sdoprobe-")})).Status(201).SaveJSON("id", "id").SaveJSON("ref", "ref")),
				step("read-back", receipt{auth(traffic.GET("/api/orders/{id}", nil)).JSONEquals("ref", "{ref}").JSONEquals("item", "pen")}),
				step("find", auth(traffic.GET("/api/orders", byRef)).ExactSet("items", "id", "{id}")),
			},
			// the id the marked create saved marks the delete; SDO then reads /api/orders/{id} back and expects 404
			Cleanup: []traffic.Step{step("delete", auth(traffic.DELETE("/api/orders/{id}", nil)))},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody, traffic.FaultSlow},
		},
		{
			ID: "call-orders", Description: "list orders, the fixture among them, and records users already have", Target: web,
			DependsOn: path, Marker: marker, // a read: GET steps only
			Users: []traffic.ProbeUser{reader}, Setup: fixture(reader),
			Steps: []traffic.Step{
				// the list as a user first sees it (default order, first page); members by name, so probe records show
				step("existing", auth(traffic.GET("/api/orders/public", nil)).ReadsExistingRecords("items", "ref", `^\S+$`)),
				step("own", auth(traffic.GET("/api/orders", traffic.Params{"ref": traffic.Const("sdoprobe-fixture-" + reader.Name)})).
					JSONEquals("items.0.item", "pen")),
			},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody, traffic.FaultSlow},
		},
		{
			ID: "later-order", Description: "an order written once is still exactly there", Target: web,
			DependsOn: path, Marker: marker,
			Users: []traffic.ProbeUser{keeper}, Setup: fixture(keeper),
			Steps: []traffic.Step{step("read", auth(traffic.GET("/api/orders", traffic.Params{"ref": traffic.Const("sdoprobe-fixture-" + keeper.Name)})).
				ExactSet("items", "ref", "sdoprobe-fixture-"+keeper.Name).JSONEquals("items.0.item", "pen"))},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody, traffic.FaultSlow},
		},
	}
}
```

`SaveJSON(path, key)` can save a string or numeric ID. Use `{key}` in a path, header, JSON body or expected value.
In a query or form, use `traffic.FromPrevious("key")` as the `Param`. To choose among several seeded fixtures,
use `FromSeed` to select an index, or `FromSeedPair` to keep two fields paired. The engine supplies the random
source (`math/rand/v2`) and an RFC3339 iteration time in `state[traffic.StateNow]`.
Go infers `SideEffect` from the methods: GET/HEAD only is a read, a write with Cleanup is write-with-cleanup.
Declare `traffic.SideEffectIdempotentWrite` for a write without Cleanup, and a side effect for a custom Endpoint.
Mark a POST that changes nothing (a search, a sign-in) with `.Reads()`. A request that names an id a marked write
saved needs no marker of its own. After a DELETE of a path a step reads, SDO GETs it and expects 404 or 410: a 2xx
delete can mean the removal is only scheduled. Where deleted records stay readable, end Cleanup with your own read.

## `.sdo/diagnostics/traffic/workloads/`

```yaml
# probe-write.yaml: the home of every write-read scenario (probe-call and probe-read-later alike)
apiVersion: sdo.dev/v1alpha1
kind: TrafficWorkload
name: probe-write
purpose: health-probe
ratePerSecond: 0.02
sampleInterval: 60s
timeout: 5s
iterationTimeout: 30s
slo: {window: 4, minSamples: 1, maxAge: 10m, maxErrorRate: 1, maxTimeoutRate: 1, maxLatency: 10s}
scenarios: [{id: write-order}]
---
# probe-fast-write.yaml: the core writes again, every 10 s
apiVersion: sdo.dev/v1alpha1
kind: TrafficWorkload
name: probe-fast-write
purpose: health-probe
ratePerSecond: 0.1
timeout: 5s
iterationTimeout: 20s
scenarios:
  - id: write-order
    slo: {maxLatency: 2s}
```

Put each workload in its own file. Schedule `call-orders` in `probe-call.yaml` and `later-order` in
`probe-read-later.yaml` using the home workload shape. Each scenario has one map entry even when several workloads run it.

## `.sdo/playbooks/health-objective/README.md`

Front matter `schema_version: 1`, `owner: responder`, `fault_class: health-objective-failure`,
`originating_incident: lifecycle-bootstrap`; then where things run, how users enter, where metrics and logs are, and the
probe costs (what a probe leaves behind, calibrated latencies).
