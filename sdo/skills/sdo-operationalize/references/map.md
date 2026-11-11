# Application map format (version 1)

The map is one YAML block at the end of the architecture file (`.sdo/arch.md`), between these marker lines.
Go writes everything else in that file; leave it byte-identical. Use logical names only, never memory paths.

````markdown
<!-- sdo:application-map:begin -->
```yaml
version: 1
components:
  - name: orders-db                 # unique; use the workload name
    workload: Deployment/orders-db  # Kind/name from the manifest inventory
    services: [orders-db]           # Services from the inventory that select this workload
    holds_data: true                # losing its contents loses user-visible data
    data: {engine: postgres, role: authoritative}   # every data store; omit for stateless workloads
  - name: orders-cache
    workload: Deployment/orders-cache
    services: [orders-cache]
    holds_data: false               # refills from orders-db on a miss
    data: {engine: redis, role: derived, derived_from: [orders-db]}
  - name: web
    workload: Deployment/web
    services: [web]
    holds_data: false
  - name: orders
    workload: Deployment/orders
    services: [orders]
    holds_data: false
  - name: metrics
    workload: Deployment/metrics
    services: [metrics]
    holds_data: false
edges:
  - {from: web, to: orders, evidence: [config, observed], source: web/config.yaml}
  - {from: orders, to: orders-db, evidence: [config], source: orders/db.go}
  - {from: orders, to: orders-cache, evidence: [config], source: orders/cache.go}
  - {from: web, to: metrics, evidence: [config], source: web/config.yaml, unprobed: fire-and-forget push; no response reaches the user}
operations:
  - id: place-order
    writes: true
    entry: {service: web, port: 8080, method: POST, path: /orders}
    path: [web, orders, orders-db]   # entry component first
  - id: list-orders
    writes: false
    entry: {service: web, port: 8080, method: GET, path: /orders}
    path: [web, orders, orders-cache, orders-db]
probes:
  - {scenario: call-list-orders, kind: call, operations: [list-orders]}
  - {scenario: write-place-order, kind: write-read, operations: [place-order, list-orders]}
  - {scenario: later-orders, kind: read-later, operations: [list-orders]}
```
<!-- sdo:application-map:end -->
````

Rules the memory check enforces:

- Names are unique; components, workloads and Services exist in the inventory; edge ends and path entries are
  component names; `entry.service` is a Service of the first path component; every path component is reachable
  from the first through edges inside the path.
- `data.engine` is one of postgres, mysql, mongodb, redis, memcached, consul, other. `data.role` is
  `authoritative` (no `derived_from`) or `derived`: `derived_from` lists authoritative components, and a derived
  component has `holds_data: false`. For an engine SDO can read (all but other), Go installs a
  `store-state-<component>` health detector on the component's workload; do not author another.
- `evidence` is a non-empty subset of `[config, manifest, observed]`: `config` is the application source or its
  config files, `manifest` the rendered manifests (an env value, a ConfigMap entry, an image, a volume), `observed`
  the live application. `source` cites the file for config evidence, the object and field for manifest evidence,
  or says how you observed the call. A component may carry the same `evidence` list for what grounds its
  `holds_data` and `data`; omit it when the source declares them.
- A probe's path is the union of its operations' paths. Its scenario's `DependsOn` names at least one Service of
  every component on that path and no other Service.
- `call` scenarios are in workload `probe-call`, `write-read` scenarios (a write side effect, at least one
  writing operation) in `probe-write`, `read-later` scenarios (read side effect, a data-holding component on the
  path) in `probe-read-later`. Each scenario is in exactly one of them and has one probe entry.
- Coverage: every edge from an entry component lies on the path of a probed operation, or carries `unprobed`, one
  line saying why no synthetic request can cross it (needing a login is not a reason: a scenario logs in as a test
  identity); every operation is in a call or write-read probe; every writing operation is in a write-read probe;
  every data-holding component on a writing operation's path is on a read-later probe's path. An operation broken
  in the application as deployed is covered by a probe whose workload scenario is declared `knownBroken`
  (`knownBroken: {reason: <one line>, failsWith: ["404"]}`); at most half of the operations may be.

## Seeded map

When the architecture has no map, Go seeds one from the rendered manifests, all with evidence `manifest`: one
component per workload, store roles by well-known image (`holds_data` from the engine or persistent volumes), edges
from env values and ConfigMap entries that name a Service address, and no operations or probes. A call whose
address only a Secret holds is listed above the map as a possible edge (from the env var, Secret and key names,
never the value): confirm it live and map it. The entry
candidates above the map list exposed or uncalled Services with the workload's health endpoints. Start from the seed:
drop components that are not part of the app, fix roles and edges, add the operations and probes. With source,
confirm each entry against it. Without source, use the manifests, what you know of well-known software and the live
application (routes, connections, logs): keep `manifest` or `observed` evidence and never claim `config`.

## Procedures

`procedures` (optional, after `probes`) lists the operations procedures the application itself ships: admin CLI
tasks, self-checks, maintenance commands and Jobs its docs or source describe. Responders and playbooks reach for
them before hand-made changes, and run a `check` one as diagnosis evidence.

```yaml
procedures:
  - id: orders-check                  # unique logical ID
    component: orders                 # runs in this component's workload
    container: app                    # container of its Pods
    command: [orders-admin, check, "--scope={{scope}}"]   # argv; {{name}} is a declared parameter
    params:
      - {name: scope, type: string, pattern: "[a-z]+", default: all}   # type string|int|bool; required: true
    effect: check                     # check (read-only) | repair (idempotent) | data (changes records) | long
    verify: exits 0 and prints no errors   # optional: how to tell it worked
    timeout: 2m                       # optional, at most 5m
    source: docs/operations.md        # file, doc or manifest object that declares it
  - {id: orders-migrate, job: Job/orders-migrate, effect: data, source: "manifest Job/orders-migrate"}
```

A procedure runs in a component (`component`, `container`, `command`) or re-creates a Job of the inventory from its
template (`job`, no command or params). Every placeholder names a declared parameter and every parameter appears in
the command; a string value without `pattern` is one plain word. Go seeds each Job of the manifests with effect
`long`: correct the class, and add the procedures only source and docs reveal.
