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

- When the base has no region, Go upkeep seeds one on the architecture commit from the rendered manifests
  (`sdo/operator/memory/mapseed`): components, data roles by well-known store image, `holds_data` from the engine or
  persistent volumes, edges from env values and ConfigMap addresses, no operations or probes, all with evidence
  `manifest`. Entry candidates (exposed or uncalled Services, with the workload's HTTP health probes) go in the
  Go-owned text above the region. An application deployed without source thus still gets data roles and Go's
  store readers, and the designer starts from the seed. Once a region exists it is the designer's.
- Go upkeep keeps writing the front matter and the manifest inventory. The detection designer edits only the text
  between the markers (`SDO_ACTOR=health_judge`, `SDO_PHASE=initial`). Everything outside the markers must be
  byte-identical to the base for that actor. If a later Go rewrite of the inventory happens, it copies the
  region verbatim.
- At most one region. No region means "no map": the coverage check reports `map.missing` as a non-blocking note
  until the designer has written one, so existing memory keeps validating. (Go seeds a new architecture whose manifests declare workloads.)
- The map holds logical names only (Service and workload names, operation and scenario IDs), never `.sdo/` paths.

## Schema

```yaml
version: 1
components:                       # one per application workload the designer considers part of the app
  - name: post-storage-mongodb    # logical ID, [a-z0-9-], unique; use the workload name
    workload: Deployment/post-storage-mongodb   # Kind/name present in the manifest inventory
    services: [post-storage-mongodb]            # Service names (in the inventory) that select this workload
    holds_data: true              # losing its contents loses user-visible data (not a cache that refills)
    evidence: [manifest]          # optional: what grounds holds_data and data, same values as an edge's;
                                  # omitted when the application source declares them
    data: {engine: mongodb, role: authoritative}   # every data store: engine and authoritative/derived
edges:                            # directed: caller -> callee
  - from: post-storage-service    # component name
    to: post-storage-mongodb      # component name
    evidence: [config]            # non-empty subset of [config, manifest, observed]
    source: config/service-config.json   # config: application source path that declares the call;
                                         # manifest: the rendered object and field that name the callee;
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

1. `version` is 1. Unknown fields are rejected. Evidence values are `config` (application source or its config
   files), `manifest` (the rendered manifests: image, volume, env value, ConfigMap entry; all there is for an
   application deployed without source) and `observed` (the live application).
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
- An operation broken in the application as deployed is covered by a probe whose health-probe workload scenario
  is declared `knownBroken` (a short reason and the baseline `failsWith` statuses). The probe still runs; the
  traffic detector never fires on it nor waits for it to pass, and notes (O3, never a gate) when it starts
  working or fails differently. Each such probe must exercise an operation no gated probe covers, and these
  operations are at most half of the map's (`map.known-broken`); a workload keeps at least one gated scenario.

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
URL or manifest text. An input SDO cannot read before the run (for example a value from accepted source) counts as
reaching every `holds_data` component. `EnsureDatabaseUser` is `store-setting` on its target; the typed store
actions declare their own class. The effect is recorded evidence, not a gate: the stage runs these actions, and only
the Pod-replacing rule above refuses. The shared memory check has no matching rule because a program binds its
workload from live findings, so its source does not say which one it will touch.

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

## Recovery no probe crossed

A Pod becoming Ready shows that its process answers its own probe, not that users' requests through it succeed. As a
health-clear closure freezes in an application that has health probes, the controller lists the Services the
incident's findings reach (the Service itself, or the Services selecting a named Pod or workload). If no qualified
health-probe scenario enters or crosses one of them (`Target` or `DependsOn`), the closure commits `unconfirmed` with
reason `OBSERVATION_INCOMPLETE`, and a pending `controller.recovery-unprobed` entry in `detector_history` names the
Service. Reflection gets it as `recovery_unprobed` and may not publish learning from the closure. A Service a probe
crosses needs no extra rule: that probe's detector is a health detector, so a probe that keeps failing keeps the
incident open. Map coverage (`map.uncovered-edge`) is what puts such a probe on every edge from an entry component.

Repairs that change records in place do not make the closure unconfirmed. The closure records each applied data
effect on a `holds_data` component as a `data effect:` note on the outcome (`source_check_notes`), and `sdo verify`
lists the same lines in `data_effects[]`. For an `unknown` or `data-write` effect the note says whether a backup of
this incident held the records it could change. At closure a recorded plan keeps its objects but not the text of its
bound values, so only the Pods and objects it names count there.

## What the map does not hold

No fault kinds, no edge semantics (cache, queue, replica), no thresholds and no live measurements. Detectors stay
the generic traffic detector Go already installs per health-probe workload, so the three workloads give exactly
one persistent-failure detector per probe kind (`traffic-probe-call`, `traffic-probe-write`,
`traffic-probe-read-later`), each localizing by the failing scenario's `DependsOn`.
