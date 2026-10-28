# Program notes

Facts behind the authoring steps. Signatures are on the [SDK card](sdk-card.md).

## Per-cause work

Go settles covered repairs and their corpus cases before reflection; answer only the open questions in the
per-cause work. A verified cause with no recorded repair on its own resources is recorded, and the other
causes are still learned. A program that still matches the verified fixed capture needs refinement: Go keeps
that capture as a NoMatch regression case (`CapturedFixedNoMatchPhase`), never a positive case.

## Bindings and values

- `bindings.Objects[role]` holds resource identities; `bindings.Parameter(spec, name)` returns a scalar
  parameter or its default. Required scalar parameters need a `Default` so findings can bind them. Diagnose
  and Mitigate declare identical parameters. Two roles of one Kind never bind and are refused at save.
- An emergency literal is a declared parameter the responder supplies and justifies.

## Source restoration

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
other users, and is irreversible. Build replay from recorded open `DatabaseUsers` replies. Add
`RestartRollout` for a dependent Deployment when recovery needs a reconnect.

## Routing and Service ports

Declared container ports choose probe candidates; they never prove a listener. A numeric `targetPort`
repair needs successful TCP probes at the candidate port and refused probes at the faulty port on the
selected ready Pods; anything ambiguous stays Unknown. `RemoveServicePort` removes exactly one port by
name or by port and protocol; missing or ambiguous entries refuse, and the last port needs `AllowEmpty`.

## Acceptance

The broker repeats the memory check. Compilation, imports, changed-source formatting, ownership and
receipt provenance are required; replay outcomes, missing captures and detector coverage are information.
