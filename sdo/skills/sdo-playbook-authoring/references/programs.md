# Direct program authoring

Programs implement the [generated program SDK](program-sdk.md). Diagnose reads a bound resource view;
mitigation returns typed actions. Shared read helpers come from the diagnostics module's library packages.
Keep names, ports and images in bindings or observed state, never in the learned predicate.

A program directory contains diagnose.go, optional mitigate.go, Go tests and a README with responder ownership,
fault class and originating incident. Preserve accepted provenance and add its logical ID to the playbook index.
The worker's authoring reference tool supplies the current worked routing example alongside this guide.

## Evidence and corpus

Use the captures actually present in the incident evidence inventory. The current controller captures the
incident-open boundary and the independently verified fixed boundary; it does not promise intermediate
snapshots. A symptom observed later in the responder session cannot be backfilled into the open capture.
An unsupported positive case is an evidence gap.

The [generated runtime contract](../../sdo-memory/references/runtime.md) distinguishes capture filenames
from provenance phase values. The corpus creation tool copies open, fixed and primitive recordings exactly,
uses fixed bytes for the healthy snapshot, and derives digests and incident provenance. Supply the confirmed
program IDs, affected bindings and a healthy same-kind peer from fixed evidence. Accepted cases are immutable;
new observations get a new case ID. Synthetic tightening cases need a concrete negative reason and negative
program labels; they cannot replace captured positive evidence.

Diagnosis replay requires Match at open and NoMatch at fixed and healthy peers, including consistently renamed
resources, labels, ports and images. Prove matching and near-miss cases. Unknown or failed primitive reads stay
Unknown; empty or absent observations never prove success or refusal.

## Source restoration

Keep source-restoration mitigators for image, readiness and ConfigMap faults when diagnosis supports them.
ReconcileFromSource and Rollback express desired state from accepted source or history. External effects may
produce an informational skipped replay result; this proves no repair and earns no origin credit. Diagnosis
replay remains mandatory. SetField-only plans must replay to NoMatch after applying their patches.
Never delete a valid mitigator merely to eliminate a skipped-effect note.

## Routing

Declared container ports identify probe candidates; they do not prove listeners. A numeric targetPort repair
requires captured successful TCP probes at the candidate port and refused probes at the faulty port for the
selected ready Pods. An unready Pod, a second working target, multiple candidates or absent probes leave the
relation ambiguous. Choose healthy peers with recorded observations.

Keep a SetField mitigator when these observations prove the replacement and replay clears the diagnosis.
Replay may retain direct-Pod probe observations across a Service-only patch when the complete Pod snapshot is
unchanged. It never treats a Service probe or a fixed-phase answer as proof of a projected repair.

## Acceptance

The complete memory check compiles, vets and tests the authored sources, validates imports, ownership,
provenance, captured bytes and README policy, and runs generated corpus, rename and mitigation replay.
Fix diagnostics in the final proposal files; the broker checks the same files independently.

## Go-owned per-cause work

The supplied work list preserves the responder's confirmed resource groups. Covered repairs and their corpus cases are already settled; address only typed questions. Unwritten bindings, unproven plan equivalence, and writes without a matched binding earn no credit. Unassigned observations remain ungrouped questions. Do not infer merge/split decisions.

A program that still matches the verified fixed capture has a definite false match. Go seeds that exact capture as negative evidence and preserves it through edits; the failing NoMatch replay is the question to resolve. The [generated replay API](../../sdo-detector-authoring/references/replay.md) defines CapturedFixedNoMatchPhase for this observed negative evidence. It is never a positive incident-open phase, a synthetic positive case or a trust credit.
