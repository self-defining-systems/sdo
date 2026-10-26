# Direct program authoring

Programs implement the [generated program SDK](program-sdk.md). Diagnose reads a bound resource view;
mitigation returns typed actions. Import shared SDK helpers (`sdk/observe`, `sdk/routing`) instead of copying them into `.sdo/lib`.
Keep names, ports and images in bindings or observed state, never in the learned predicate.

Detectors detect continuously; playbook Diagnose runs only while handling their findings, using one finding-bound assignment. Read additional objects through ReadView by name or relation.

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

For typed lists, selectors, pagination and CRDs, use the [recorded Kubernetes client](kubernetes-client.md).

## Evidence and replay fixtures

Learn from recorded responder actions and live observations already present in the incident session.
Build a fixture from the available snapshot and those observations; a missing controller open/fixed
capture must not prevent learning. Do not invent successful primitive replies. Supply root_cause_program_ids
and object bindings for the confirmed cause. Keep originating incident attribution in labels.provenance.incident_id;
snapshot digests and a protected capture mount are not required. Include accepted.json when the executed
mitigation reads accepted source. For replay, open.json represents the fault and recordings.json contains
its observed primitive replies; fixed.json and healthy.json remain readable context, not repair proof.

Every independently verified cause requires diagnose and mitigate programs, a README describing fault
handling, and a detector that references its playbook. One detector may reference several playbooks.
Replay runs incident detectors on the recorded fault, binds through production finding selection, requires Diagnose Match, executes mitigation on that same binding, and reaches NoMatch; the healthy baseline must remain quiet.
Compilation and that repeat path are save-time learning checks. Rename, healthy-peer and controller
capture-provenance checks are offline analysis, not publication prerequisites. Preserve accepted fixture bytes.

## Source restoration

Keep source-restoration mitigators for image, readiness and ConfigMap faults when diagnosis supports them.
Prefer a narrow, schema-checked SetField when diagnosis identifies the damaged field; named list selectors and
ConfigMap JSON/YAML entries preserve unrelated values. Use `program.FromSource(ref, path)` for an accepted-source field value,
and ReconcileFromSource only when restoring the whole source-owned object is justified. Structured effects use the ordinary namespace-scoped executor.
ReconcileFromSource and Rollback express desired state from accepted source or explicit history. Supply
accepted manifests in accepted.json for source-based replay. Replay must execute the proposed action and
clear diagnosis; an unavailable external effect requires a typed SDK gap instead of a skipped success.
CreateFromSource restores only a named missing object present in accepted source. DeleteObject removes a
named object in the incident namespace; disclose deletion as irreversible, retain its receipt, and never
auto-recreate it. Live verification requires direct current UID/absence evidence and independent health checks.

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

The broker enforces artifact ownership and verified-cause coverage independently of the worker's claim.
Programs must compile and mitigation must clear diagnosis in replay. Failed replay is rejected and retried
with diagnostics. A skipped effect is not replay success. The only learning exception is a typed SDK gap
with the confirmed-cause resources and a concrete reason. Missing controller captures do not justify a decline.

## Go-owned per-cause work

The supplied work list preserves the responder's confirmed resource groups. Covered repairs and their corpus cases are already settled; address only typed questions. Unwritten bindings, unproven plan equivalence, and writes without a matched binding earn no credit. Unassigned observations remain ungrouped questions. Do not infer merge/split decisions.

A program that still matches the verified fixed capture has a definite false match. Go seeds that exact capture as negative evidence and preserves it through edits; the failing NoMatch replay is the question to resolve. The [generated replay API](../../sdo-detector-authoring/references/replay.md) defines CapturedFixedNoMatchPhase for this observed negative evidence. It is never a positive incident-open phase, a synthetic positive case or a trust credit.

Go's missing-observation flags describe optional controller captures. They do not block fixtures built
from session observations and cannot justify an evidence_gap decline. An unavailable SDK capability can
be reported with a typed SDK gap naming its cause resources, primitive, reason and scratch script.

Automatic plans require no source-policy file, reviewed-fixture digest, content predicate or credential-delta approval. Explicit FromSource selectors and CreateFromSource read accepted manifests at the incident commit; other repairs may use observed or declared parameter values. DeleteObject does not require source absence. Legacy field actions retain their declared scope checks. The [general action vocabulary](general-actions.md) covers arbitrary nonsecret resources and subresources through Kubernetes RBAC.

Compilation and repair replay remain mandatory. Direct observation and independent post-apply verification still judge the executed plan. A partially failed write batch compensates its reversible writes; deletes and exec are explicitly irreversible and are never auto-recreated or replayed. UID, conditional-write, lifecycle exclusivity and rollback conflict checks remain correctness constraints.

At incident open, Go sends all matching diagnoses and unexplained findings to the responder. The agent selects mitigations and decides rollback. Verified program counters are information only; failed verification retains completed writes.

Receipts report value provenance (`accepted-source`, `parameter`, `observed`, `incident-namespace`), not an administrator grant. JSON/YAML ConfigMap entry edits preserve unrelated values and numeric precision, including untouched large integers and exponent literals; document serialization may normalize formatting.

`spec.template.spec.dnsPolicy` is a schema-checked SetField path for Deployment and StatefulSet source
restoration. A DNS policy repair does not remove an injected dnsConfig; if that field was absent from
accepted source, use general JSON patch removal rather than inventing an empty source value.

An undeclared or unavailable scoped observation remains Unknown.

A missing final `env[name=NAME]` SetField target may insert the accepted EnvVar with the same name, including
when the env list was omitted. Existing entries must be unique; a different value name or missing parent
container refuses. This preserves unrelated entries and does not change credential-channel authorization.

## Removing one Service port

Use `RemoveServicePort{Ref: service, Name: observedName, Reason: reason}` to remove exactly one
unwanted port without restoring a stale source Service. Alternatively select `Port` and optional
`Protocol` (TCP, UDP or SCTP); name and number are mutually exclusive. Missing and ambiguous entries
refuse before writing. The last port is protected unless `AllowEmpty` is explicit; API validation still
applies. The executor uses UID/version-conditional patches, proves the resulting list by direct read,
and retains the preimage for conflict-checked compensation. Keep a hand-written open/fixed replay case
showing the removed listener and preservation of other ports and Service fields.

## Database catalogs

`ReadView.DatabaseUsers(DatabaseTarget)` observes nonsecret user names and directly granted roles.
Targets select `Engine: program.MongoDB`, a bound application Pod, container, database, and optional
`Shell` (`mongo` default or `mongosh`). Optional `DatabaseAuth` names the authentication database and
username/password references. `CredentialRef` selects exactly one of `Env: name` or
`ConfigMap: &program.ConfigMapCredentialRef{Name: name, Key: key, Assignment: optionalName}`.
ConfigMaps are read in the target Pod namespace. An omitted assignment selects the entire key;
an assignment selects exactly one literal `NAME=value` line (shell or dotenv, quoted or bare).
Missing/duplicate assignments, expansions and command substitution fail with a typed reference error.
Only the executor resolves ConfigMap bytes and sends them through exec stdin; programs, receipts,
recordings and corpus retain references only. Env refs resolve inside the container, including
Secret-backed env. Direct Secret keys are unavailable under the existing metadata-only RBAC;
never add a grant or read Secret data to work around it.

`EnsureDatabaseUser{Target, User, Roles, Password: reference, Reason}` creates a missing
user with a password resolved privately by the executor. For an existing user it restores exact direct roles
and preserves its password. Other users remain unchanged. Local unauthenticated bootstrap requires the
database's existing policy; the action creates no Kubernetes grants. It is irreversible and never
re-executed from a durable receipt. Diagnosis must inspect the typed catalog and propagate unavailable
reads as Unknown. Build replay from recorded open `DatabaseUsers` results; fixed-phase replies never
prove a repair. The simulator changes only the named user's direct roles/catalog membership and does
not simulate credentials, arbitrary Exec, authentication policy, connectivity, or server availability.
Include `RestartRollout` for a dependent bound Deployment when recovery also needs a reconnect.
Independent live health verification remains required.

General object edits, replayable commands/HTTP, and one-off scripts use the [general action vocabulary](general-actions.md).


Keep RunScript bash inline in Mitigate;
application inputs are relation-derived `program.Value` entries in Env, resolved
at apply time and injected as process environment. Typed actions remain optional.
Scripts default to 60 seconds, bounded by five minutes and the incident budget;
script and each output stream retain at most 64 KiB.

For opaque scripts, label `script_origins` in the corpus case with the script
SHA-256 digest mapped to its original successful repair receipt ID. The
controller adds timed `RepairReceipt` observations to protected recordings before
reflection. Open Diagnose must Match and fixed Diagnose must NoMatch with valid
bindings. The replay report uses `observed-transition`: it does not execute bash,
claim exact external-effect simulation, or assert the rewritten script ran.
The live apply→Diagnose check verifies that version; global health owns closure.
Preimages and API receipts are retained, with explicit gaps for opaque effects;
undo remains an explicit responder decision.
