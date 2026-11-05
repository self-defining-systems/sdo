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
  non-empty edge evidence from `[config, observed]`, logical names only (no memory paths). Each probe entry lists the operations
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
- An edge no operation path crosses is noted, never refused: add the operation that uses it, or keep it as a
  known gap. Enforced by: `map.unexercised-edge`.
