# Program notes

Facts behind the authoring steps. Signatures are on the [SDK card](sdk-card.md).

## Per-cause work

Go settles covered repairs before reflection; answer only the open questions in the per-cause work. A verified cause with no recorded repair on its own resources is recorded, and the other
causes are still learned.
<!-- replay:on -->
A program that still matches the verified fixed capture needs refinement: Go keeps
that capture as a NoMatch regression case (`CapturedFixedNoMatchPhase`), never a positive case.
<!-- /replay:on -->

A program directory contains diagnose.go, optional mitigate.go, Go tests and a README with responder ownership,
fault class and originating incident. Preserve accepted provenance; SDO lists it in the generated playbook index.
The authoring reference tool supplies the routing example.

## Bindings and values

- `bindings.Objects[role]` holds resource identities; `bindings.Parameter(spec, name)` returns a scalar
  parameter or its default. Required scalar parameters need a `Default` so findings can bind them. Diagnose
  and Mitigate declare identical parameters. Two roles of one Kind never bind and are refused at save.
- An emergency literal is a declared parameter the responder supplies and justifies.

## Source restoration

Choose playbooks by failure mechanism. Admission mutation, resource exhaustion and old-revision
blockage need separate diagnoses and steps even on one Deployment; link them from findings and explain
ordering in READMEs. Keep actions together only when one repair needs them. Unrelated unavailable reads
must not turn a decidable source comparison into Unknown. Preserve earlier cases when refining variants.
Judge a pending stage by its final receipt: a later verified settlement preserves the working step;
a handoff snapshot alone does not justify rewriting it.

- `SetField` restores one schema-checked field; named list selectors (`env[name=X]`) and ConfigMap
  JSON/YAML entries (`DocumentFormat`, `DocumentPath`) preserve unrelated values. A missing final
  `env[name=X]` entry is inserted.
- `ReconcileFromSource` restores a whole source-owned object and removes reported additions;
  `CreateFromSource` restores a named missing object. `DeleteObject` is irreversible; a delete-style Diagnose
  matches on `diff.PresentButNotInSource()`, which turns false after the delete.
- A `dnsPolicy` repair does not remove an injected `dnsConfig`; remove that with a JSON patch.

## Stalled rollouts

Mitigate may run again: while a rollout stays pending the runtime re-plans on fresh observations, at most
three follow-ups, until a plan repeats. `StalledRolloutRelease` retires (`RetireOldRevision`) each old
revision holding the place of a replacement that cannot be placed or created, selected by ownerReference
and revision; it returns nothing otherwise. Scaling a current revision is refused (`replicaset_scale_refused`).

## Pods and commands

`DeletePod` checks UID/version; a terminating Pod counts as deleted and is never recreated on undo.
`ScaleWorkload` verifies `spec.replicas`. `Exec` runs as the controller, reports only the exit code and
earns no repair credit when refused. `RunScript` defaults to 60 s, at most 5 min; script and each output
stream keep at most 64 KiB. Its bytes are checked against this program's linked receipts at plan time;
unlinked bytes are refused as `script_unprovenanced`.

## Database catalogs

`DatabaseUsers(DatabaseTarget)` reads nonsecret user names and direct roles of one MongoDB catalog on a
bound Pod (`Shell` mongo or mongosh). `CredentialRef` is exactly one of `Env` or `ConfigMap{Name, Key,
Assignment}` in the Pod's namespace; only the executor resolves it, through exec stdin.
`EnsureDatabaseUser` creates a missing user or restores its exact direct roles, keeps existing passwords and
other users, and is irreversible. Supply the complete desired role set: omitted direct roles are removed,
so translating an additive grant must preserve the other accepted roles.
<!-- replay:on -->
Build replay from recorded open `DatabaseUsers` replies.
<!-- /replay:on -->
Add
`RestartRollout` for a dependent Deployment when recovery needs a reconnect.

## Routing and Service ports

Declared container ports choose probe candidates; they never prove a listener. A numeric `targetPort`
repair needs successful TCP probes at the candidate port and refused probes at the faulty port on the
selected ready Pods; anything ambiguous stays Unknown. `RemoveServicePort` removes exactly one port by
name or by port and protocol; missing or ambiguous entries refuse, and the last port needs `AllowEmpty`.

## Webhook trust

Derive serving Secret references from `AdmissionWebhooksFor` on the failing Namespace or workload.
Plan binding can complete missing `SyncWebhookCA` reference fields from one exact observed webhook and
serving-key relation. Explicit fields stay fixed; missing or ambiguous relations require declared inputs.
A whole-Secret mount does not identify its certificate key. Declare any needed scalar in `Spec` with a
usable default or derive it from observations; never depend on undeclared values appearing in bindings.

## Acceptance

The broker repeats the memory check. Compilation, imports, changed-source formatting, ownership and
receipt provenance are required.
<!-- replay:on -->
Replay outcomes, missing captures and detector coverage are information.
<!-- /replay:on -->
