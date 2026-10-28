# Slot validity and cleanup evidence

## sdo3 infrastructure outcomes and composites

Inspect the CSV's top-level `infra_error` before judging submissions, token
artifacts or strict receipts. A non-empty infrastructure error invalidates the
measurement even if an aggregate oracle says false; report the recorded reason
and exclude it from agent scores. `injection_invalid` is terminal and intentionally
ungraded, with benchmark `Injection.*` evidence and no required agent artifacts.
Do not accuse the agent of failing to submit or count an injection as an incident
response. A `fault_i_infra_error` alone does not override an already graded
aggregate failure: preserve the harness's top-level policy.

Per-problem CSVs can appear directly below `results/<agent>/<problem>/` when no
agent attempt started, as well as below `run_<attempt>/`. Pipeline state
`invalid_infra` preserves these terminal outcomes across resume; it is distinct
from `completed`, `agent_failure`, and missing artifacts. Persistent-controller
drain still runs for the pipeline, but terminal infrastructure rows need no
strict receipt. Workspace chaining uses the previous stage's actual workspace.

Composite verdict exports retain `faults_total`, `faults_fixed`,
`faults_diagnosed`, `composite_valid`, and `per_fault_details`, alongside aggregate
success and judge-free TTD/TTM. Missing values remain unknown. Retain per-fault
infrastructure details without silently converting them to agent failures.


Fastloop invalid injections carry `infra_error=injection_invalid` and
`injection_evidence`; they have no response, oracle grade or learning credit.
Summaries exclude them from incident denominators and expose
`infrastructure_outcomes` separately. For lanes reset by row-start recreation, `cleanup_skipped_reason=row_start_tier2`
also applies to partial/invalid injection cleanup; interrupts and partial composite
failures do not invoke per-fault undo. Missing cleanup flags on these rows do not
invalidate an already sampled grade. The next row recreates the dirty lane.
Other warm-recovery paths still require protocol-v2 full cleanup and a successful
independent recovery oracle; `recovery_error` stops those streams.

Explicit `up --redeploy` and dirty-lane bring-up share the row reset transaction.
`wipe-receipt.json` may report `tier2_trigger=explicit_redeploy`. The durable
last-injection journal is pending before recreation can write; only healthy source
deployment and completed recapture clear it. Missing or interrupted history stays
dirty. A plain application reapply cannot clear unknown data history; only the full
recreation transaction establishes that fact. Known clean warm lanes retain the
fast path. Control state reset stays separate. After every writer stops it retires
controller state and all worker request, result and execution records, so a later
identical request cannot reuse a result computed against a deleted checkout. Those
records survive only in per-incident checkpoints; the native ledger is unchanged.
On a shared-memory lane the wipe re-provisions the private repository claim and its PV
together; a PV still bound to a deleted claim's UID is recreated with it, never left Released.

## Ordered mixed-app dispatch

`python -m benchmarks.sregym.fastloop.dispatch` drives exactly the first ten
entries of a supplied 50-slot `sequence_plan.json`, without regrouping by app.
Each app binding pins its independently verified seed and saved fastloop
environment; seed creation and lifecycle verification remain separate gates.
Each slot runs in a fresh process against that app's persistent workspace and
controller. `run --slot-offset` makes agent IDs, receipt/checkpoint directories
and incident row indices use the global slot number.

The dispatcher writes `run_manifest.json` (`sdo.mixed-dispatch/v1`) with the full
50-entry plan, SHA-256 of the original plan file bytes, app seed/initial/latest
accepted commits, controller-state paths, and each attempted slot's before/after
accepted commits and status. It also aggregates the slot rows into
`incidents.jsonl`/`.csv`; the detailed artifacts stay in each app environment's
`results/<dispatch-run-id>-slot-<NNN>/`. A failed child or recovery leaves the
manifest failed and prevents any subsequent slot. Output directories cannot be
reused; restarting a failed stream is an explicit new experiment.

`full_cleanup_verified=true` on RC1 and newer drivers means successful protocol-v2
`seed-compatible/v1` recovery, application baseline reconciliation, successful
post-seed client refresh, then healthy workloads and completed rollouts. Older
drivers did not require seed compatibility or client refresh; inspect the run's
driver/harness revisions before treating their true flag as full credential recovery.
Legacy rows default false. Dispatch requires
this field, a successful post-recovery oracle, no skipped cleanup and no error
or infrastructure outcome. Aggregate-only faults remain supported; missing
per-fault probe timing does not bypass the cleanup gate. DT1's shared detached
launcher owns supervision and its process run record; dispatch adds no launcher.

## App-namespace drift after recovery (`eval_notes`, `confounds`)

The worker deletes exactly what the injection window created; only an injection object it cannot
remove fails recovery. Any other kind+name difference from the pre-injection inventory of the app
namespace (a responder's NetworkPolicy, a ConfigMap an accepted repair added, a deleted Service) is a
values-free `eval_notes` entry (`app_namespace_inventory_drift`, `not_in_baseline`,
`missing_from_baseline`) on that row and a `confounds` entry on the next row. Neither blocks the lane.
Treat a row with `confounds` as starting from a changed app; check whether the drift is SDO's
accepted repair before calling it a harness leftover.

## Recovery after any closure outcome (`recovery_trigger`)

Fault recovery runs once the row is over, whatever happened to SDO's closure. `recovery_trigger`
names the path: `natural_closure` (reflection acknowledged), `closure_failed` (SDO permanently
failed or retired the closure; the rejected receipt holds the reason), `reflection_deadline` (SDO
was still reflecting at the deadline; intake was paused, the closure left to SDO, and the lane
stops after the row), or `no_incident`. A running reflection is waited for, never aborted. Rows
before this field could skip recovery after a failed closure and leave injected objects (for
example a ValidatingWebhookConfiguration) in the next row.

After recovery, the recovery oracle is sampled until it passes or a 300 s settle budget ends (the
admission stabilization rule); `recovery_oracle_samples` lists each sample's seconds and success.
"recovery residue" means the last sample still failed at the budget. Earlier rows took one sample,
so traffic that lagged a just-recovered app could be reported as residue.

## Credential fault followed by immutable seed restoration

**Signature:** official fault recovery passes, but restoring the old seed makes database clients fail.

Ordinary secret-rotation recovery retains the rotated backend credential. Warm-slot cleanup needs
the harness's `seed-compatible/v1` contract: reset external state, restore application seed manifests,
refresh client connections/rollouts, and verify health. SDO's fault driver owns this ordered operation;
the harness owns the credential reset and refresh hooks. It rejects older harness recovery before
applying a seed, even without `--require-full-cleanup`. Retries after partial restores are supported.
Do not count `full_cleanup_verified` until the refresh, health and rollout gates pass. Recovery errors
identify numeric component indices and known lifecycle statuses (`error` or `timeout`); raw harness
exception text is excluded because it may contain credential material. Investigate by Secret name/key
names and sanitized phase status only; never inspect Secret data or credentials.


Social-network supported reset rebuilds the post and timeline fixture together
with writers stopped. `fastloop/social_fixture.py` checks every Mongo, user Redis
and home Redis reference against post storage before resuming the original
traffic. Treat fixture restoration as **eval setup**, never a responder repair or
learning outcome. A contract failure invalidates admission even if sampled HTTP
passes; the checker rejects empty fixtures and preserves exact int64 IDs.

For bound PVCs, a rendered source manifest may omit the admission-defaulted
`storageClassName`. Restore must retain that field from the identity-matched
installed capture when source omits it. An explicit source class (including the
empty string) remains authoritative. Replacing the complete source spec without
this merge rejects an otherwise healthy bound claim as an immutable-field change.
On a reused lane, explicitly retire released prior applications through supported
teardown, then restore the app and check for foreign applications before health/traffic
admission and injection. App-only reset does not retire foreign namespaces.
Never bypass the foreign-application guard with an added namespace allowlist.

Before any post-incident maintenance, reset or app-only restore, require the
native reflection drain to finish durably. An interrupted drain is a held lane,
including when its saved evidence is loaded after a runner restart; file
existence is not success. The outer runner must apply this gate to every arm.
Fastloop reset and restore also reject a controller record with a pending
incident, before touching the controller, repository or application. Preserve
the original grade and worker evidence when learning fails; an unacknowledged
reflection proposal is not accepted memory for a repeat.

Namespace recovery prerequisite failures are **eval** blockers before injection.
`eval: namespace recovery cannot restore prerequisites:` names every unavailable
reference as `[namespace/]Kind/name`. Capture now includes ServiceAccounts and
namespace RBAC; legacy omissions may be filled from hash-verified accepted
rendered manifests without replacing sealed installed workload specs. Secret
payloads and unowned cluster resources are never restored. A named missing
Secret after namespace loss is unsupported recovery, not an SDO mitigation
failure or a health timeout; uninjected encounter metrics remain unknown.

## Row-start tier 2 supersedes post-row recovery

For single-row SDO runs with row-start wipes, `cleanup_skipped_reason` is
`row_start_tier2`: the legacy recovery paths above do not run. No per-fault
restore, post-closure recovery wait, or recovery-oracle settling gates the grade.
The durable attempt marker retains injected row IDs, so the next row recreates
the app namespace/PVCs under tier 2 and records its trigger in `wipe-receipt.json`.
Entry, restore-invariant, pre-injection baseline and injection-validity checks
still run. Warm streams without row-start wipes retain their recovery fences.
