# Application map (format v1)

The detection designer (wire name `health_judge`) maps the application before it writes probes. The map is
data that Go can check: which user operations exist and which of them write, which components hold data, which
component calls which, and which probe covers what. It lives in the architecture artifact, so every task that
already carries the architecture text (incident, reflection, refresh) sees it with no new plumbing.

## Where it lives

One fenced YAML block in `.sdo/arch.md`, after the Go inventory, between two marker lines:

````markdown
<!-- sdo:application-map:begin -->
```yaml
version: 1
...
```
<!-- sdo:application-map:end -->
````

- Go upkeep keeps writing the front matter and the manifest inventory. The detection designer edits only the text
  between the markers (`SDO_ACTOR=health_judge`, `SDO_PHASE=initial`). Everything outside the markers must be
  byte-identical to the base for that actor. If a later Go rewrite of the inventory happens, it copies the
  region verbatim.
- At most one region. No region means "no map": the coverage check reports `map.missing` as a non-blocking note
  until the designer has written one, so existing memory keeps validating.
- The map holds logical names only (Service and workload names, operation and scenario IDs), never `.sdo/` paths.

## Schema

```yaml
version: 1
components:                       # one per application workload the designer considers part of the app
  - name: post-storage-mongodb    # logical ID, [a-z0-9-], unique; use the workload name
    workload: Deployment/post-storage-mongodb   # Kind/name present in the manifest inventory
    services: [post-storage-mongodb]            # Service names (in the inventory) that select this workload
    holds_data: true              # losing its contents loses user-visible data (not a cache that refills)
edges:                            # directed: caller -> callee
  - from: post-storage-service    # component name
    to: post-storage-mongodb      # component name
    evidence: [config]            # non-empty subset of [config, observed]
    source: config/service-config.json   # config: application source path that declares the call;
                                         # observed-only: one line saying how it was observed
operations:                       # user-facing operations at the entry point
  - id: compose-post              # logical ID, [a-z0-9-], unique
    writes: true                  # changes stored state
    entry: {service: nginx-thrift, port: 8080, method: POST, path: /wrk2-api/post/compose}
    path: [nginx-thrift, compose-post-service, post-storage-service, post-storage-mongodb]
                                  # components the operation reaches, entry component first
probes:                           # one entry per probe scenario in the health workloads
  - scenario: write-compose-post  # traffic scenario ID
    kind: write-read              # call | write-read | read-later
    operations: [compose-post, read-user-timeline]   # operations its steps exercise
```

Field rules (all blocking unless marked note):

1. `version` is 1. Unknown fields are rejected.
2. Component `workload` and every `services` entry resolve in the accepted source inventory. Names are unique.
3. Edge endpoints are component names; no self-edges; no duplicate (from, to).
4. Operation `entry.service` is a Service of `path[0]`. `entry.method` is an HTTP method. Every component in
   `path` is reachable from `path[0]` through edges whose both ends are in `path`.
5. A probe's derived path is the union of its operations' paths. The scenario's `DependsOn` must name at least
   one Service of every component on that path and no Service outside it.
6. Probe kinds and their scenarios:
   - `call`: listed in health-probe workload `probe-call`; any side effect.
   - `write-read`: listed in health-probe workload `probe-write`; side effect `idempotent-write` or
     `write-with-cleanup`; at least one of its operations has `writes: true`.
   - `read-later`: listed in health-probe workload `probe-read-later`; side effect `read`; its path contains
     at least one `holds_data` component; it has `Setup` steps that write the fixture it reads (once per prober
     start, as an idempotent ensure step), so it never depends on data set up by hand. Verify bursts share
     the continuous probe's completed setup, so a lost fixture stays failing until the data returns; only a
     controller restart writes it again.
   Each scenario appears in exactly one of these workloads, and every scenario in them has a probe entry.
   A health-probe workload may hold only write scenarios (the SDK no longer requires a read scenario per
   health probe; that change ships with the designer skill).

## Coverage (what the shared memory check verifies)

- Every operation is in some `call` or `write-read` probe (a write-read probe also calls its operations).
- Every operation with `writes: true` is in some `write-read` probe.
- Every `holds_data` component that lies on the path of a writing operation lies on the path of some
  `read-later` probe.
- Every edge's endpoints lie together on some operation path (note `map.unexercised-edge` otherwise).

Each gap is one diagnostic naming the operation, component or edge (`map.uncovered-operation`,
`map.uncovered-write`, `map.uncovered-data`, `map.path-mismatch`, `map.unknown-name`, `map.schema`). They block
for the designer's commits once a map exists; for other actors they are notes, so reflection never fails on a
designer gap.

## What the deterministic stage does with it

The stage refuses a matched program whose plan would restart, delete, scale down or otherwise rewrite a workload the
accepted map marks `holds_data`, or one of its Pods. The attempt is recorded as `skipped` with `skip_reason:
holds_data` and an error naming the action, and the finding goes to the responder, who may act by hand. Creating the
workload and scaling it up run as before.

External actions get a data effect per component they reach: `read`, `derived-invalidate`, `store-setting`,
`data-write` or `unknown` (`program.DataEffect`). Go computes it and an author cannot lower it. An `Exec`,
`EnsureExec`, `EnsureHTTP`, `RunScript` or `RunJob` is `unknown` on the component whose Pod it runs in and on each
component its inputs name: a Service, workload or Pod given as a value's object, or named in its command, script,
URL or manifest text. An input the stage cannot read before the run (for example a value from accepted source)
counts as reaching every `holds_data` component. `EnsureDatabaseUser` is `store-setting` on its target. On a
`holds_data` component the stage allows only `read`; `derived-invalidate`, `store-setting` and `data-write` wait for
the map roles, typed restores and backups that make them safe, and `unknown` is refused. This is a rule about kinds
of action, not a trust gate. Without a map nothing is refused. The shared memory check has no
matching rule because a program binds its workload from live findings, so its source does not say which one it will
touch.

## Repairs that may have lost data

As a health-clear closure freezes, the controller checks the Pods of every `holds_data` workload. If one was created,
or (with no `emptyDir`) one of its containers started, since the incident opened, and the Pod has only volumes that
die with it, the closure commits `unconfirmed` with reason `DATA_MAY_BE_LOST` and a detail naming the workload. The
signal is the Pod's own timestamps, so a program, a hand repair and a crash all count, and it survives a controller
restart with no tracking. Health cannot rule the loss out because probes read only the data they wrote. The outcome
says why through a pending `controller.data-at-risk` entry in `detector_history`, as any unconfirmed closure names its
blockers. Reflection gets it as `data_at_risk` and may not publish learning from the closure, and the responder sees the
same fact as `data_at_risk[]` from `sdo verify`. A Pod replaced before the
incident opened does not count.

The same closure check also covers repairs that may have changed records in place. If an applied action had an
`unknown` or `data-write` effect on a `holds_data` component and no backup was taken first (the SDK has no backup
action yet, so none is), the closure commits `unconfirmed` with reason `DATA_MAY_BE_LOST` and a detail naming the
workload and the action. The responder may still make such a repair; it just cannot feed learning. A typed
`store-setting` change (`EnsureDatabaseUser`) does not count. At closure a recorded plan keeps its objects but not
the text of its bound values, so only the Pods and objects it names count there.

## What the map does not hold

No fault kinds, no edge semantics (cache, queue, replica), no thresholds and no live measurements. Detectors stay
the generic traffic detector Go already installs per health-probe workload, so the three workloads give exactly
one persistent-failure detector per probe kind (`traffic-probe-call`, `traffic-probe-write`,
`traffic-probe-read-later`), each localizing by the failing scenario's `DependsOn`.
