---
name: sdo-operations-map
description: Write and read the application map in .sdo/arch.md. Covers entries, operations and their correct results, components, edges, existing records, workers, probes, reasons and the schema.
---

# The operations map

The map is one YAML block between the map markers in `.sdo/arch.md`. It says what users do, how each operation
reaches the components it needs, and how SDO observes and tests it. `sdo map` prints it (components and
operations, one operation's path, or a component's neighbours). Full format: [map](references/map.md); the
memory check enforces the [rules](references/rules.md).

## What goes in

- **Entries**: one record per exposed entry (Ingress backend, Gateway route, LoadBalancer or NodePort Service)
  and per internal entry (background workers, schedules): the request metric, a selector, the names of its
  route, method and status labels, and the error pattern ([observability](../sdo-observability/SKILL.md)).
- **Operations**: every user journey. Find each entry point in source (routes, API specs, proxy config,
  non-HTTP entries, workers, schedules). Each operation has an `entry` (service, port, method, path), a `route`
  when its metric's route value differs from the path, `writes: true` when it changes stored state, a `path` of
  every component it reaches (entry first), and `correct`: one line on what a correct result looks like.
- **Components**: one per workload, with the Services that select it; `holds_data: true` when it is the only
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
  - service: frontend                 # exposed: Ingress backend, Gateway route, LoadBalancer, NodePort
    metric: http_server_requests_total
    selector: {service: frontend}
    labels: {route: route, method: method, status: code}
    errors: '5..'
  - service: internal                 # background workers and schedules
    metric: jobs_total
    labels: {route: queue, status: outcome}
    errors: 'failed|dead'
operations:
  - id: create-order
    writes: true
    entry: {service: frontend, port: 80, method: POST, path: /api/orders}
    path: [frontend, orders, postgres]
    correct: "201 with an id; reading it back returns the same items and total"
  - id: send-receipt
    writes: true
    entry: {service: internal, path: receipts}
    path: [worker, redis]
    correct: "a receipt exists within 30 s of the order"
probes:
  - {scenario: create-then-read, kind: write-read, operations: [create-order]}
  - {scenario: order-gets-receipt, kind: read-later, operations: [send-receipt]}
reasons:
  - {item: tests-fail-broken, subject: send-receipt, reason: "worker retries for 10 min before failing"}
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
