# SDO experiment evidence reference

Use this reference to correlate durable production evidence with SREGym artifacts. Field sets may grow; inspect the run's schema/version and repository models before assuming optional fields.

## Evidence hierarchy

For production-lifecycle claims, use this order:

1. strict production receipt;
2. controller-owned outcomes and durable runtime state;
3. broker incident ledger and attributable Git commits;
4. lifecycle provenance and validated operational memory;
5. controller, responder, validator, and harness logs;
6. agent-specific trajectories.

A benchmark result can establish the harness verdict. It cannot by itself establish memory ownership, independent verification, reflection, rollout, or cleanup.

## Lifecycle provenance

Path: `.sdo/lifecycle-provenance.yaml`

Expected sections:

- `deployer`: session ID, source commit, topology fingerprint, resource inventory, and architecture summary. The model returns only the commit, fingerprint, and summary; the controller attaches the resource inventory deterministically from tracked manifests, so the inventory is a controller fact rather than model output.
- `active_topology` (when supplied by the deployment adapter): sorted kind/name references for the deployed source variant and required non-optional ConfigMap dependencies.
- `health_judge`: the accepted final detector artifact.
- `health_judge_rounds`: ordered structured attempts with distinct session IDs, round numbers, objective digest, covered resources, source, and tests.
- `validation` (when the validator exposes an immutable identity): the diagnostics-tree digest, immutable validator identity, and `sdo.lifecycle-validation/v1` attestation schema. An exact match permits validation reuse without recompilation; a missing or mismatched field requires fresh isolated validation.

Correlate the deployer source commit with Git and `arch.md`. Correlate the objective digest with the exact text in `goal.md`. When `active_topology` is present, verify that `covered_resources` contains no inactive source variants. A reused lifecycle is credible only when current source topology, active topology, and detector validation still match.

## Detailed evidence by workflow step

- When interpreting mitigation and learning, read [outcome records](outcome-records.md).
- When tracing acceptance and restart behavior, read [native broker and memory](native-broker.md).
- When validating benchmark claims and costs, read [receipts and benchmark results](benchmark-results.md).
- For fastloop rows and learned-program confirmation, read [fastloop results](fastloop-results.md).
- When reconstructing controller timing, read [telemetry and trajectories](telemetry.md).

## Abstaining program refinement questions

Go-owned `reflection_eligibility.causes[].items[]` can carry
`reason: program_refinement` with `disposition: question`. The item identifies
an existing program/revision and bindings that returned `NoMatch` or `Unknown`
for an object in the confirmed cause that a successful repair action wrote.
`evaluation` retains the typed `ProgramRun`, including verdict evidence and
errors. Repeated runs with the same program, revision, bindings and verdict are
represented by the first evaluation; distinct verdicts and revisions remain
separate. A matching evaluation for the same revision and binding (or with an
omitted revision on either evaluation) uses the
existing matched-program work instead, so post-repair `NoMatch` is not treated
as stale coverage.

The item asks reflection to consider the incident's open capture as the positive
case and extend/fix the program or explain why abstention should remain. Its
`evidence_phase: open` names that requested capture, not the evaluation's time;
`ProgramRun` has no phase timestamp. This question does not itself establish a
false negative, label a positive corpus case, or grant trust. Unavailable
observations can still justify safe abstention. Repaired objects without an
eligible evaluation retain the generic `unbound_write` question. Historical
outcomes lack these candidates; inspect their raw program runs before concluding
that no existing program evaluated the object.
