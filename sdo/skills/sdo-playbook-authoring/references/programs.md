# Direct program authoring

Programs implement the [generated program SDK](program-sdk.md). Diagnose reads a bound resource view;
mitigation returns typed actions. Import shared SDK helpers (`sdk/observe`, `sdk/routing`) instead of copying them into `.sdo/lib`.
Keep names, ports and images in bindings or observed state, never in the learned predicate.

Snapshot-only detectors may import their application's playbook package and promote its Diagnoser with
`program.DetectorFromProgram`. Set `DetectorSpec.Playbooks` to the actual logical playbook IDs; detector IDs
may differ. Findings preserve those declared references. Tests may import application playbooks to exercise
their public contracts; production helper and playbook packages cannot import other playbook packages.

Accepted Mitigators become selectable options in the responder menu, a convenience over direct commands.
Declare object and typed value parameters and group related actions into one plan. Raw registered actions
also appear as generic one-action plans. The ordinary PlanExecutor runs them with the controller's existing
RBAC and always-on checks; the menu adds no ledger, fence, quiescence or provenance precondition.
The same responder session receives what ran, per-action results, value provenance and errors and may
choose another option or repair directly. Up to five rounds share the incident allowance. Intermediate
reports are not independent outcomes or program trust credit.

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

Offline closed-loop matrix fixtures may add `health_impact: true|false` to `labels.json` (default true).
False permits health detectors to stay silent on latent faults; it never relaxes incident detector or Diagnose
family expectations or detector silence on Healthy. Detector roles use existing `Spec().Class`; cross-family
health firing is reported as overlap. See the [closed-loop report contract](../../../operator/runtime/replay/README.md).

## Source restoration

Keep source-restoration mitigators for image, readiness and ConfigMap faults when diagnosis supports them.
Prefer a narrow, schema-checked SetField when diagnosis identifies the damaged field; named list selectors and
ConfigMap JSON/YAML entries preserve unrelated values. Use `program.FromSource(ref, path)` for an accepted-source field value,
and ReconcileFromSource only when restoring the whole source-owned object is justified. Structured effects use the ordinary namespace-scoped executor.
ReconcileFromSource and Rollback express desired state from accepted source or history. External effects may
produce an informational skipped replay result; this proves no repair and earns no origin credit. Diagnosis
replay remains mandatory. Observed and parameter-based SetField plans must replay to NoMatch after applying their patches; source-selected fields require unavailable accepted-source evidence and report a skipped effect.
CreateFromSource restores only a named missing object present in accepted source. DeleteObject removes a named object in the incident namespace without a source-absence gate; disclose deletion as irreversible, retain
its receipt, and never auto-recreate it. Both are external effects: skipped replay proves no repair,
and live verification requires direct current UID/absence evidence plus independent health checks.
Never delete a valid mitigator merely to eliminate a skipped-effect note.

## Values and runtime actions

`SetField.Value` is an opaque `program.Value`: use `FromSource(ref, path)` for the incident-bound accepted
manifest, `FromObserved(ref, path)` for this invocation's ReadView snapshot, or `FromParam(name)` for a
declared scalar parameter. Raw Go literals do not compile, and zero values fail validation. The executor
resolves at apply time and reports the source commit/path, observed resourceVersion/path, or parameter name.
Secret-typed references fail before lookup; no Secret-derived resolved value belongs in reports or receipts.
Observed values remain bound to their program invocation when several plans share one apply batch.

Use `bindings.Objects[name]` for resource identities and `bindings.Values[name]` for scalar inputs.
Declare value parameters in `ProgramSpec.Params` with `ValueKind` (`StringValue`, `IntValue`, `QuantityValue`,
`DurationValue`, or `BoolValue`) and an optional `Default`. Keep object `Kind` empty for a value parameter.
`ValidateBindings` rejects wrong types, unknown names, duplicate declarations, and mixed object/value kinds.
Use `bindings.Parameter(spec, name)` to retrieve a validated binding or its default. Quantities and durations
are strings in Kubernetes quantity and Go duration syntax; booleans and integers retain their scalar types.
Named constants such as a scale amount are defaulted parameters. An emergency literal is a declared
parameter the responder supplies and justifies; a permanent intended-state change requires a source repair.

`DeletePod{Ref}` deletes one named Pod with UID/version preconditions and verifies absence or a replacement
UID. It never recreates a Pod during undo. `ScaleWorkload{Ref, Replicas}` accepts a Deployment or StatefulSet,
resolves a nonnegative integer replica count, and directly verifies `spec.replicas`. Partial failures undo
reversible fields from captured pre-images; irreversible effects remain disclosed.

`Exec{Pod, Container, Command: []program.Value{...}}` derives every command argument through values. The
controller's existing Kubernetes RBAC must allow the exec subresource; no grant is created by a plan.
Callers supply that controller transport through `RuntimeOptions.PlanExecConfig`; nil reports refused. Exec
can reach mounted Secret files, so stdout and stderr are never requested, returned, or stored. Only the exit
code is reported. Refused or uncertain execution earns no write or health-verification credit, and a durable
receipt never authorizes command replay after restart.

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

Automatic plans require no source-policy file, reviewed-fixture digest, content predicate or credential-delta approval. Explicit FromSource selectors and CreateFromSource read accepted manifests at the incident commit; other repairs may use observed or declared parameter values. DeleteObject does not require source absence. All actions remain within the controller-derived incident namespace, with Kubernetes RBAC and schema checks. Cluster-scoped webhook mutations are outside this scope.

Save-time build, vet, tests and repair replay remain mandatory. Direct observation and independent post-apply verification still judge the executed plan. Failed fast-path attempts compensate all reversible writes before responder dispatch; deletes and exec are explicitly irreversible and are never auto-recreated or replayed. UID, conditional-write, lifecycle exclusivity and rollback conflict checks remain correctness constraints.

The fast path defaults on and chooses one trusted program/binding by findings covered, then stable program ID/bindings order. Uncovered findings go to the responder after whole-incident verification; failed verification rolls back the attempt. Success credit survives broker-validated revisions; real fast-path failure still revokes trust.

Receipts report value provenance (`accepted-source`, `parameter`, `observed`, `incident-namespace`), not an administrator grant. JSON/YAML ConfigMap entry edits preserve unrelated values and numeric precision, including untouched large integers and exponent literals; document serialization may normalize formatting.

`spec.template.spec.dnsPolicy` is a schema-checked SetField path for Deployment and StatefulSet source
restoration. A DNS policy repair does not remove an injected dnsConfig; if that field was absent from
accepted source, report the missing removal capability rather than inventing an empty source value.

Promoted snapshot-only detectors preserve the configured scoped observation source; an undeclared or
unavailable foreign read remains Unknown. They still cannot perform live logs, probes or source diffs.

A missing final `env[name=NAME]` SetField target may insert the accepted EnvVar with the same name, including
when the env list was omitted. Existing entries must be unique; a different value name or missing parent
container refuses. This preserves unrelated entries and does not change credential-channel authorization.
