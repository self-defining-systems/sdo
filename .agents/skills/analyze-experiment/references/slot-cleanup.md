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
`infrastructure_outcomes` separately. `recovery_error` stops the stream even with
continue-on-error: no baseline repair or reset can erase failed full recovery.
The driver requests protocol v2 full cleanup before restoring its owned baseline.

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
