# Application map rules

Every rule names the validator rule IDs that enforce it. `sdo-operator memory check` prints the ID of each
diagnostic; look it up here. The rules block the onboarding agent's commits once a map exists; other actors
see them as notes. The diagnostic names the operation, component, edge or probe kind to fix.

## The map region

- The map is one YAML block between the map markers at the end of the architecture file. Write it there when it
  is absent; until then the check only notes its absence. Enforced by: `map.missing`.
- The onboarding agent changes nothing in the architecture file outside the markers and never removes them.
  Enforced by: `ownership.map-outside-region`.
- Only the onboarding agent changes the map; every other actor leaves the region byte-identical. Enforced by:
  `ownership.map-region-owner`.

## Format and names

- The block is strict YAML of the [map](map.md) format: version 1, no unknown fields or duplicate keys, unique
  names of lowercase letters, digits and `-`, an entry with a Service, a port and a path starting with `/`,
  non-empty edge evidence (and optional component evidence) from `[config, manifest, observed]`, logical names only (no memory paths). Each probe entry lists the operations
  its steps exercise, and each scenario of the three probe workloads has exactly one probe entry of the matching
  kind. A write-read scenario writes; a read-later scenario is a read, reaches a data-holding component, and
  creates the data it reads in its own setup steps. A probe scenario checks more than statuses in at least one
  step ([strict probes](probes.md)). Enforced by: `map.schema`.
- Components, workloads and Services exist in the manifest inventory; edge ends, path entries and probe
  operations name map entries. Enforced by: `map.unknown-name`.
- An operation's path starts at the component whose Service is its entry and reaches every component through
  edges inside the path. A probe scenario's `DependsOn` names a Service of every component on its probe's path
  and no other Service. Enforced by: `map.path-mismatch`.

## Coverage

- Every operation is in a call or write-read probe. Enforced by: `map.uncovered-operation`.
- Every writing operation is in a write-read probe. Enforced by: `map.uncovered-write`.
- Every data-holding component on a writing operation's path is on a read-later probe's path. Enforced by:
  `map.uncovered-data`.
- A health-probe scenario declared `knownBroken` (with a short `reason` and the baseline `failsWith`) covers the
  operations no gated probe exercises, without a write-read or read-later probe; it never gates health. Each such
  probe must exercise one operation no gated probe covers, and these operations are at most half of the map's.
  Enforced by: `map.known-broken`.
- User entry Services are the Services the manifests expose outside the cluster (NodePort, LoadBalancer, an
  Ingress backend; the inventory's `exposure:` lines), metrics ports excluded. When none is mapped, they are the
  entry Services of your declared operations. A Service nothing calls is not an entry by itself. Every component
  reachable from an entry component over mapped edges (or a listed possible edge) is covered by a probe whose
  operation enters at a user entry Service and includes the component on its path, or the component records
  `unprobed: <why>` when no synthetic request can reach it. An operation entering elsewhere (a metrics scrape, an
  internal API) covers nothing. Enforced by: `map.coverage`.
- A recorded component reason is a coverage gap note in the operability report; no user test then opens an
  incident or proves recovery through that component. A probe that covers the component makes the reason wrong
  (`map.schema`). An exposed Service users never use (an operator or observability UI) records the reason on its
  component. `unprobed` on an edge is an informational note only and satisfies nothing. Enforced by:
  `map.unprobed`.
- A component that owns a user entry Service cannot be `unprobed`: declare an operation entering at its Service
  (`EXEC` for SSH commands) and probe it. More than half of the user-path components `unprobed` is rejected.
  Enforced by: `map.unprobed-entry`, `map.unprobed-share`.
- A map with a data component has at least one gated probe with a step built with
  `ReadsExistingRecords(path, field, pattern)`: it lists records users made or the deployment seeded (an empty
  list passes, a fresh install is healthy), checks every member's shape, and templates no key a write step of the
  scenario saved (open ids a list step saved). The one escape is the map-level `no_user_records: <why>`, recorded as a
  note. Enforced by: `map.records-unread`, `map.no-user-records`.
- A map with an authoritative store and no health detector implementing `appstate.Reader` is noted, never
  rejected. Enforced by: `map.readers`.
- A possible edge Go lists above the map (a Secret reference names the callee's Service; the address is unread) is
  noted until the map has the edge: confirm the call on the live application and add it. Enforced by:
  `map.possible-edge`.

## Telemetry bindings

Bindings are `.sdo/diagnostics/telemetry/sources.yaml` and `backends.yaml`; see [telemetry](telemetry.md). Only the
onboarding agent writes them, in the initial phase, and only in open mode.

- A bindings file is one strict YAML document: version 1, no unknown fields or duplicate keys, at most 32 KiB, unique
  well-formed ids, known kinds, and a complete `ready` block when present. Enforced by: `telemetry.schema`.
- A source names a registered adapter that serves every kind it lists, once each. Enforced by: `telemetry.adapter`.
- An endpoint is a Service in the control or application namespace or an allowlisted host, with no IP literal,
  `localhost`, userinfo, query or fragment; publish URLs follow the same rule. Enforced by: `telemetry.endpoint`.
- A credential is a granted name, never a value. Enforced by: `telemetry.credential`.
- Adapter options contain only keys the adapter defines. Enforced by: `telemetry.options`.
- At most one default source per kind across operator-provided and agent-bound sources. Enforced by: `telemetry.default`.
- A backend names an offered recipe and uses only capabilities the environment granted and the recipe declares; a
  source's `backend` is defined in `backends.yaml`. Enforced by: `telemetry.backend`.
- Pinned and floor environments admit no agent bindings. Enforced by: `telemetry.mode`.
- Go assigns origins and operator-provided ids are not redefined. Enforced by: `telemetry.origin`.
- Every detector query resolves to a bound source, and an alerts consumer has an alert-capable source. Enforced by:
  `telemetry.unbound`.
- The resolved source serves the query's kind. Enforced by: `telemetry.kind`.

## Data checks

Data checks are `.sdo/diagnostics/data/<check-id>.yaml`; see [data checks](data-checks.md). Only the
onboarding agent writes them.

- A check is strict YAML: one store, an engine with a reader, a target, exactly one of `ids`, `range` or `first`
  (at most 32 records), 1 to 16 assertions of one kind each, read fields of its engine only, a mongodb filter of
  comparison and logic operators, a postgres `where` without functions, casts or statements, and a lookup exactly
  when a reference or equivalence needs one. Enforced by: `data-check.invalid`.
- Every store a check reads is a map component whose `data.engine` is the check's engine; an equivalence compares a
  `role: derived` component with a component in its `derived_from`. Enforced by: `data-check.component`.
