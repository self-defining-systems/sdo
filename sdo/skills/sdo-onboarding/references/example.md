# Worked example

A small order service: `web` (port 80, Deployment), `orders-db` (StatefulSet, postgres), `worker` (no Service). Users
sign in with a password and place orders; a worker writes the receipt. Accounts are made by an admin CLI.

## Application map

In the seeded map, keep the component and edge entries. Add the operation and its probe, using the goal's journey ID.
This fragment maps the create operation; map the read operations the probe exercises too.

```yaml
operations:
  - id: place-order
    journey: place-and-track-order
    correct: The order and its receipt can be read back with the submitted values.
    writes: true
    entry: {service: web, port: 80, method: POST, path: /api/orders}
    path: [web, orders-db, worker]
probes:
  - {scenario: write-order, kind: write-read, operations: [place-order]}
procedures:
  - id: probe-user
    component: web
    container: app
    command: [order-admin, users, ensure, "--name={{username}}", --password-stdin]
    params: [{name: username, type: string, required: true}]
    effect: data
    probe_identity: true
    source: docs/admin.md
```

The procedure reads one password line from stdin and ensures an ordinary user without resetting a matching password.

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

	buyer  = traffic.ProbeUser{Name: "sdoprobe-buyer", As: "pw"}   // writes
	reader = traffic.ProbeUser{Name: "sdoprobe-reader", As: "pw"}  // owns the fixtures reads check
	keeper = traffic.ProbeUser{Name: "sdoprobe-keeper", As: "pw"}  // owns the read-later fixture
)

func step(name string, e traffic.Endpoint) traffic.Step { return traffic.Step{Name: name, Endpoint: e} }

// auth sends the session that login saved.
func auth(e *traffic.HTTPEndpoint) *traffic.HTTPEndpoint { return e.WithHeader("Authorization", "Bearer {token}") }

func login(u traffic.ProbeUser) traffic.Step {
	return step("login", traffic.POST("/api/login", nil).
		WithJSON(`{"user":"`+u.Name+`","password":"{pw}"}`, nil).SaveJSON("token", "token"))
}

func fixture(u traffic.ProbeUser) []traffic.Step { // Setup: ensure steps, safe to run again and from two probers
	return []traffic.Step{login(u),
		step("ensure-fixture", auth(traffic.POST("/api/orders", nil).
			WithJSON(`{"ref":"sdoprobe-fixture-`+u.Name+`","item":"pen"}`, nil)).OrExisting(409, "exists"))}
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
			DependsOn: path, SideEffect: traffic.SideEffectWriteWithCleanup, Marker: marker,
			Users: []traffic.ProbeUser{buyer}, Setup: []traffic.Step{login(buyer)},
			Steps: []traffic.Step{
				step("create", auth(traffic.POST("/api/orders", nil).WithJSON(`{"ref":"{ref}","item":"pen"}`,
					traffic.Params{"ref": traffic.Unique("sdoprobe-")})).Status(201).SaveJSON("id", "id").SaveJSON("ref", "ref")),
				step("read-back", receipt{auth(traffic.GET("/api/orders/{id}", nil)).JSONEquals("ref", "{ref}").JSONEquals("item", "pen")}),
				step("find", auth(traffic.GET("/api/orders", byRef)).ExactSet("items", "id", "{id}")),
			},
			// cleanup requests carry the marker too (here in the query); the last step reads the order gone,
			// because a delete answered 2xx may only schedule the removal
			Cleanup: []traffic.Step{
				step("delete", auth(traffic.DELETE("/api/orders/{id}", traffic.Params{"probe": traffic.Const(marker)}))),
				step("gone", auth(traffic.GET("/api/orders/{id}", traffic.Params{"probe": traffic.Const(marker)})).Status(404)),
			},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody, traffic.FaultSlow},
		},
		{
			ID: "call-orders", Description: "list orders, the fixture among them, and records users already have", Target: web,
			DependsOn: path, SideEffect: traffic.SideEffectRead, Marker: marker,
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
			DependsOn: path, SideEffect: traffic.SideEffectRead, Marker: marker,
			Users: []traffic.ProbeUser{keeper}, Setup: fixture(keeper),
			Steps: []traffic.Step{step("read", auth(traffic.GET("/api/orders", traffic.Params{"ref": traffic.Const("sdoprobe-fixture-" + keeper.Name)})).
				ExactSet("items", "ref", "sdoprobe-fixture-"+keeper.Name))},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody, traffic.FaultSlow},
		},
	}
}
```

An endpoint of your own implements `Build(ctx, *rand.Rand, State) (traffic.Request, error)` and
`Check(Response, State) Verdict`; anything pure in the standard library's encoders and hashes may be imported. A step may
set its own `Target`. `{key}` in paths, headers, JSON and wanted values is filled from State.

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

One file per workload. The map then has `probes: [{scenario: write-order, kind: write-read, operations: [place-order]}, ...]`
with one entry per scenario, whatever workloads run it.

## `.sdo/playbooks/health-objective/README.md`

Front matter `schema_version: 1`, `owner: responder`, `fault_class: health-objective-failure`,
`originating_incident: lifecycle-bootstrap`; then where things run, how users enter, where metrics and logs are, and the
probe costs (what a probe leaves behind, calibrated latencies).
