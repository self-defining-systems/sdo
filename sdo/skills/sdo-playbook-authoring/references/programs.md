# Direct program authoring

Programs implement the [generated program SDK](program-sdk.md). Diagnose reads a bound resource view;
mitigation returns typed actions. Import shared SDK helpers (`sdk/observe`, `sdk/routing`) instead of copying them into `.sdo/lib`.
Keep names, ports and images in bindings or observed state, never in the learned predicate.

A program directory contains diagnose.go, optional mitigate.go, Go tests and a README with responder ownership,
fault class and originating incident. Preserve accepted provenance and add its logical ID to the playbook index.
The worker's authoring reference tool supplies the current worked routing example alongside this guide.

## Evidence and corpus

Use the captures actually present in the incident evidence inventory. Choose an available open boundary
that contains the confirmed cause and the independently verified fixed boundary. Supported phase values
do not promise that a capture exists. Never backfill a later symptom into an earlier capture.
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
Prefer a narrow, schema-checked SetField when diagnosis identifies the damaged field; named list selectors and
ConfigMap JSON/YAML entries preserve unrelated values. Use ValueFromSource for an accepted-source field value,
and ReconcileFromSource only when restoring the whole source-owned object is justified. Structured effects need
current controller-owned nonsecret provenance; accepted Git origin alone supplies no clearance.
ReconcileFromSource and Rollback express desired state from accepted source or history. External effects may
produce an informational skipped replay result; this proves no repair and earns no origin credit. Diagnosis
replay remains mandatory. Literal SetField plans must replay to NoMatch after applying their patches; source-selected fields require unavailable accepted-source evidence and report a skipped effect.
CreateFromSource restores only a named missing object present in accepted source. DeleteObject removes only
an explicitly allowed named object proven absent from that source; disclose deletion as irreversible, retain
its nonsecret receipt, and never auto-recreate it. Both are external effects: skipped replay proves no repair,
and live verification requires direct current UID/absence evidence plus independent health checks.
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

Go flags missing resource/phase observations as `evidence_gap` work items after scanning complete protected
recordings. Only those flags permit a decline for missing recordings: use the typed `decline_reason` and the
canonical "evidence gap" reason, with details in the summary. A truncated read or a recorded error does not
establish absence. The per-incident ledger records the number of distinct missing resource/phase observations;
a flag alone does not prove that learning is blocked.

Every SetField, ReconcileFromSource and Rollback compares credential channels in its pre-image and post-image, independently of path spelling. A repair must not add or change a credential channel; existing channels permit unrelated legacy repairs.

Compensation restores reversible writes even when a plan also deleted an object. The delete remains explicitly irreversible and not restored; it never triggers recreation.
