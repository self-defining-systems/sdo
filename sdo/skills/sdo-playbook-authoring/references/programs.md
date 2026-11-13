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
- A finding's named `ParameterBindings` bind only the playbooks it lists (its detector's `possiblePlaybooks`);
  findings that list none (controller drift) offer them to every program, so an incident detector that binds
  roles declares its playbooks (`detector.unscoped-binding`). Unnamed roles still bind from its resources.
- An emergency literal is a declared parameter the responder supplies and justifies.
- Generalize: for a changed field, Diagnose on the source field that differs and restore it `FromSource`;
  a Diagnose testing the value this incident showed misses the next one.
- Action fields use the compiled SDK contract. The shared memory check also rejects unknown parameter
  names in documented built-in selections against the executor descriptor; object namespaces belong in
  their typed references, not invented top-level parameters. Prefer typed executable steps over recipes.

## Localized components

A playbook binds the component it repairs (`deployment`, `statefulset`), so one program serves every component
with that failure. For a journey symptom, gate Diagnose on the component being a suspect of the failing
journeys ([localize SDK](../../sdo-detector-authoring/references/traffic.md)), then check the
repair's own evidence on it (an exec, status, log):

```go
result, err := localize.FromContext(v, probeWorkload) // ErrNoTraffic: return it (Unknown); probeWorkload is a probe workload name under diagnostics/traffic/workloads
if err != nil {
	return program.Verdict{}, err
}
if ok, why := result.Implicates(ref); !ok {
	return program.Verdict{Result: program.NoMatch, Evidence: []program.Evidence{{Resource: ref, Fact: "localization", Observed: why}}}, nil
}
```

`Implicates` (on every failing journey's path, or failing its own dials; a healthy journey that also lists
the component never clears it) alone is not a diagnosis; a healthy, source-matching workload is not evidence either. Find the
workload's Pods by owner (ReplicaSet, then Pod) for `Command{Pod: ...}` instead of naming a Pod.

Nothing in the program or its detector names a component: the second Redis with the same fault must reuse the
playbook, not copy it. Derive per-component values from the bound role, match every failing journey through it
(not one scenario ID), and let one detector bind each suspect:

```go
container := "" // the bound workload's container, not its name
for _, d := range v.Deployments() {
	if d.Namespace == ref.Namespace && d.Name == ref.Name && len(d.Spec.Template.Spec.Containers) > 0 {
		container = d.Spec.Template.Spec.Containers[0].Name
	}
}
// Detector: embeds localize.Inputs{Workload: probeWorkload}; result is localize.FromContext(snapshot, probeWorkload).
// Spec and manifest list the playbook. Key on the cause's own failure text (never a component name), not on any failure.
window, _ := traffic.WindowFrom(snapshot, probeWorkload)
matches, err := traffic.MatchFailures(window, logs.Pattern{ID: "readonly-replica", RE2: `READONLY .*read only replica`})
findings, err := traffic.NewDetector(d.Spec(), probeWorkload).Detect(ctx, snapshot)
for _, f := range findings {
	if strings.HasPrefix(f.RuleID, traffic.RuleIDPrefix) && failedWithCause(matches, f) { // matches' ScenarioID vs f
		f.Playbooks = []string{"cache-readonly-restore"}
		bound = append(bound, localize.BindSuspects(f, result, "cacheDeployment")...)
	}
}
// then keep only bound findings whose workload is of the kind the playbook acts on (e.g. an image or port check)
```

Suspects include the journey's handler and front end, so Diagnose still needs its own evidence on the bound
workload. A second incident of the same repair on another component widens this playbook's Diagnose and its
detector; a new copy is rejected (`playbook.component-copy`, `detector.component-copy`), and so is a new
component name in either (`playbook.component-literal`, `detector.component-literal`).

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
- Diagnose on typed fields: `diff.ContainerChanges()` names each changed container by kind (init or
  app) and name, and `diff.Field(path).Differs()` sees only fields source declares, never server
  defaults. `diff.Text` is the whole document, so a substring matches any container; use it only as
  Evidence. Memory check notes Text matching as `program.diff-text-match`; rewrite such programs.
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

## More program rules

- Mitigate returns to accepted or observed state; intended-state changes are source repairs. Prefer a
  narrow `SetField`; `ReconcileFromSource` restores a whole object. After a workload write, append
  `program.StalledRolloutRelease(v, ref)`.
- A recorded `kubectl exec <pod>` becomes `EnsureExec` on a Pod of the bound workload, never a Pod name.
- Typed store actions declare their effect: `StoreBackup` (read), `InvalidateDerived` (derived-invalidate, recorded
  with each `derived_from` source's live store state), `RepairData` and `RestoreBackup` (data-write; a repair's
  backup is optional and recorded). A learned cache flush is clearer as an
  `InvalidateDerived` than an `EnsureExec`, because SDO records more about it.
- A changed store setting, write block, role, principal, validator or index is clearer as
  `RestoreStoreItem{Target, Change}` (store-setting) per entry of `storestate.Changes(view)` that
  `storestate.Restorable` accepts than as an `EnsureExec` with a hand-written inverse: SDO restores its own
  baseline value, records the prior value and verifies it. `CheckStoreCredential` (read) tests a client's
  credential; `StoreChanges.Holders` groups a store's connections by peer workload and state.
- To make the app pick up a change in a store, choose by where the stale copy lives: a cache the map marks
  `data.role: derived` from the store you wrote, and that only refills from it (`AppMap.Derived`), gets
  `InvalidateDerived` on the written scope; any other copy (in-process memory, a pool, a primary that is not
  derived) gets `RestartRollout` of the components with a map edge to the written store. `holds_data` no longer
  refuses a restart. There is no wait action: a copy that expires on its own is judged by the post-apply health
  wait, so do not add a sleep.
- Credentials stay references (env or ConfigMap key); never read Secret data. Learn into incident playbooks. The detection designer owns `health-objective` and health detectors: never edit
  them; when its generic program applied and hand repairs followed, encode those in an incident playbook.
- Resolve stores by relation: `v.AppMap()` (`ComponentOf`, `Derived`), `program.StoreTargetFor`,
  `datacheck.Verdicts(v)`; never name a component.
- An out-of-reach cause: `Verdict{Match, CauseClass, Reach: program.ReachOutOfReach}` with no Mitigate or a
  sole `program.Escalate`; SDO closes it `escalated`.
- `program.ProposeSourceChange` (last in a plan) becomes a responder patch after verified recovery.
