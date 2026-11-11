# Application map rules

Every rule names the validator rule IDs that enforce it. `sdo-operator memory check` prints the ID of each
diagnostic; look it up here. The rules block the detection designer's commits once a map exists; other actors
see them as notes. The diagnostic names the operation, component, edge or probe kind to fix.

## The map region

- The map is one YAML block between the map markers at the end of the architecture file. Write it there when it
  is absent; until then the check only notes its absence. Enforced by: `map.missing`.
- The detection designer changes nothing in the architecture file outside the markers and never removes them.
  Enforced by: `ownership.map-outside-region`.
- Only the detection designer changes the map; every other actor leaves the region byte-identical. Enforced by:
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
- Every edge from an entry component (the first component of an operation's path, or a component whose Service no
  edge calls) lies on the path of an operation some probe exercises, or records `unprobed: <why>` when no
  synthetic request can cross it. Enforced by: `map.uncovered-edge`. A recorded reason is a coverage gap note in
  the operability report, and a probe that crosses the edge makes the reason wrong (`map.schema`). Enforced by:
  `map.unprobed-edge`.
- Any other edge no operation path crosses is noted, never refused: add the operation that uses it, or keep it as
  a known gap. Enforced by: `map.unexercised-edge`.

## Telemetry bindings

Bindings are `.sdo/diagnostics/telemetry/sources.yaml` and `backends.yaml`; see [telemetry](telemetry.md). Only the
detection designer writes them, in the initial phase, and only in open mode.

- A bindings file is one strict YAML document: version 1, no unknown fields or duplicate keys, at most 32 KiB, unique
  well-formed ids, known kinds, and a complete `ready` block when present. Enforced by: `telemetry.schema`.
- A source names a registered adapter that serves every kind it lists, once each. Enforced by: `telemetry.adapter`.
- An endpoint is a Service in the control or application namespace or an allowlisted host, with no IP literal,
  `localhost`, userinfo, query or fragment; publish URLs follow the same rule. Enforced by: `telemetry.endpoint`.
- A credential is a granted name, never a value. Enforced by: `telemetry.credential`.
- Adapter options contain only keys the adapter defines. Enforced by: `telemetry.options`.
- At most one default source per kind across operator-provided and designer sources. Enforced by: `telemetry.default`.
- A backend names an offered recipe and uses only capabilities the environment granted and the recipe declares; a
  source's `backend` is defined in `backends.yaml`. Enforced by: `telemetry.backend`.
- Pinned and floor environments admit no designer bindings. Enforced by: `telemetry.mode`.
- Go assigns origins and operator-provided ids are not redefined. Enforced by: `telemetry.origin`.
- Every detector query resolves to a bound source, and an alerts consumer has an alert-capable source. Enforced by:
  `telemetry.unbound`.
- The resolved source serves the query's kind. Enforced by: `telemetry.kind`.

## Data checks

Data checks are `.sdo/diagnostics/data/<check-id>.yaml`; see [data checks](data-checks.md). Only the detection
designer writes them, in the initial phase.

- A check is strict YAML: one store, an engine with a reader, a target, exactly one of `ids`, `range` or `first`
  (at most 32 records), 1 to 16 assertions of one kind each, read fields of its engine only, a mongodb filter of
  comparison and logic operators, a postgres `where` without functions, casts or statements, and a lookup exactly
  when a reference or equivalence needs one. Enforced by: `data-check.invalid`.
- Every store a check reads is a map component whose `data.engine` is the check's engine; an equivalence compares a
  `role: derived` component with a component in its `derived_from`. Enforced by: `data-check.component`.

## Log signals

Log signals are `.sdo/diagnostics/logs/<signal-id>.yaml`; see [log signals](log-signals.md). Only the detection
designer writes them, in the initial phase.

- A signal is strict YAML: 1 to 8 components, an optional `window` from 30s to 5m, 1 to 8 `signals` and `expect`
  patterns together with unique IDs, each a valid RE2 that does not match an empty line; all log signals together
  hold at most 48 patterns. Enforced by: `log-signal.invalid`.
- Every component is an application-map component with a workload. Enforced by: `log-signal.component`.
