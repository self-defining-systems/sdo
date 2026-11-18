---
name: sdo-operations-map
description: Write and read the application map in .sdo/arch.md. Covers entries, operations and their correct results, components, edges, existing records, workers, probes, reasons and the schema.
---

# The operations map

The map is one YAML block between the map markers in `.sdo/arch.md`. It says what users do, how each operation
reaches the components it needs, and how SDO observes and tests it. It lives in `.sdo/arch.md` (components and
operations and their paths). Full format: [map](references/map.md); the
memory check enforces the [rules](references/rules.md).

## What goes in

- **Entries**: one record per exposed entry (Ingress backend, Gateway route, LoadBalancer or NodePort Service)
  and per internal entry (background workers, schedules): the request metric, a selector, the names of its
  route, method and status labels, and the error pattern ([observability](../sdo-observability/SKILL.md)).
- **Operations**: every user journey. Find each entry point in source (routes, API specs, proxy config,
  non-HTTP entries, workers, schedules). Each operation has an `entry` (service, port, method, path), a `route`
  when its metric's route value differs from the path, `writes: true` when it changes stored state, a `path` of
  every component it reaches (entry first), `triggers` (the background job classes it sets off; background work
  is a step of the journey that triggers it, not an operation), and `correct`: one line on what a correct result looks like.
- **Given journeys**: when the goal lists user journeys (goal.md front matter, and "User journeys:" in the goal text), each is covered by at least one operation carrying `journey: <id>`; do not invent ids or rename them, and do not name a journey the goal does not list.
- **Where journeys come from**: source, and the app's own tests (end-to-end, API and integration tests, fixtures
  and seed data show what users do and what a correct result is). Where the repo ships end-to-end tests that can
  target a deployment, run them against yours to see which routes real flows use.
- **Routes**: `routes.list` (component, container, command) prints the application's routes, one `METHOD route`
  per line. Every listed route is a journey's route or sits in `routes.excluded` with a reason.
- **Components**: one per workload, with the Services that select it and `observe` pointers (`metrics`, `logs`, `trace_service`); `holds_data: true` when it is the only
  place some user-visible data lives; every store gets a `data` entry.
- **Edges**: caller to callee, with evidence. Ground them in code (a client address, an env value) and in
  traces from the live application. Add an edge you observe but did not map; recheck one you never observe.
- **Existing records**: probes read records users already have; otherwise `no_user_records: <why>`.
- **Probes**: each scenario, its kind and the operations it exercises. An operation's tests are the probes
  that list it ([detectors](../sdo-detectors/SKILL.md)).
- **Reasons**: for a red check item you cannot fix: item, subject and one line why. Never for
  `deployed-healthy` or `quiet-when-healthy`.

## Schema of the new parts

```yaml
entries:
  - service: web                 # exposed: Ingress backend, Gateway route, LoadBalancer, NodePort
    metric: http_server_requests_total
    selector: {service: web}
    labels: {route: route, method: method, status: code}
    errors: '5..'
  - service: internal                 # job metrics: the route label is the job class
    metric: jobs_total
    labels: {route: queue, status: outcome}
    errors: 'failed|dead'
operations:
  - id: create-order
    writes: true
    entry: {service: web, port: 80, method: POST, path: /api/orders}
    path: [web, orders, postgres, worker]
    triggers: [ReceiptJob]
    correct: "201 with an id; reading it back returns the same items; a receipt exists within 30 s"
probes:
  - {scenario: create-then-receipt, kind: read-later, operations: [create-order]}
reasons:
  - {item: background-covered, subject: PruneJob, reason: "periodic maintenance"}
```

`route` defaults to `entry.path`. Components, edges, `unprobed`, `no_user_records`, procedures and the
coverage rules keep their format ([map](references/map.md)).

## Keeping it true

- Start from Go's seeded map when there is none ([seeded map](references/map.md#seeded-map)), then confirm each
  entry against source and the live application.
- Every route value seen on an entry metric must belong to a declared operation; an unexplained value is a
  missing operation or a mislabelled metric.
- Record the operations procedures the application ships ([procedures](references/map.md#procedures)).
- Only the text between the markers is authored; Go writes the rest of `.sdo/arch.md`.
