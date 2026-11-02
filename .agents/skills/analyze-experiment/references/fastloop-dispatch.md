# Fastloop dispatch evidence

## sdo3 fault discovery and tracking

The default capability gate is `strict`. An explicitly permissive benchmark
suite attempts unreviewed or unsupported faults without asserting support.
Fastloop `run_manifest.json` (`sdo.fastloop-run/v1`) records
`fault_capabilities.mode` and one entry per fault in injection order. Incident
JSONL/CSV rows retain `capability_mode` and `fault_capabilities`, including failed
attempts and invalid injections. Join by `problem_id` and `declared_index`; each
entry retains class/key, required and missing capabilities, and `supported`,
`unreviewed`, or `unsupported` status. Never interpret permissive acceptance as a
reviewed capability, successful repair, or attributable timing probe. Tracking
requirements and independent grading remain separate gates. Runner manifests
also retain this evidence per checked suite under `fault_capabilities` (and in
their preflight facts). Older rows without this evidence have unknown review
status. The permissions opt-in is separate from capability policy.

New runs record `fault_descriptions.json` beside `incidents.jsonl`, keyed by
problem ID. Each `sdo.sregym-fault-description/v1` document preserves fault class,
key, parameters, declared index and actual `injection_order`; catalog decoys are
excluded. A stream entry may join several problem IDs, each contributing several
faults. Use these recorded descriptions in composite sequence analysis; never
reconstruct identities from a hard-coded map or a different harness revision.

`tracking_supported=false` means no attributable probe exists for at least one
fault (or probe identities collide). The official aggregate oracle still grades
the result. Such composite reports contain empty `resolved_s`/timeline and no
per-fault resolution credit; the table displays `untracked`. Do not interpret
missing probe timing as an unresolved fault, an inert injection, or zero latency.
An explicit `--require-fault-tracking` launch rejects unsupported tracking before
deployment. Capability validation separately rejects repair/snapshot scope that
SDO lacks, including namespace PSA labels, quotas and admission webhooks.


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
Before recovery, an incident SDO opened after the graded one closed gets a
bounded wait (10 min) to close; each wait is in `post_closure_waits`
(follow-up incident IDs, seconds, `closed`). A wait that times out stops the
row with `RecoveryWaitTimeout` in `recovery_error`.
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

### Fresh source seed acceptance

A seed's objective/endpoints-only health capture does not establish a green
accepted detector bundle. Compile the entire accepted diagnostics projection
and run `sdo-controller --evaluate-once --accept-seed --namespace <namespace>
--prober-url <live-prober-url>`. Retain its JSON and exit status: any finding or
evaluation error rejects acceptance, and reachability cannot be skipped. Keep
historical seed proofs immutable. Scoped build-input declarations must be
installed before fresh builds and recorded with independent rollout identities;
measure a manifest-only source edit through real broker/lock reconciliation
before reporting L6 continuity.

For infrastructure validity, mixed-app slot dispatch and full-cleanup gates, read
[slot cleanup evidence](slot-cleanup.md).

## Lane image delivery and retention

`image_delivery.jsonl` records `success` for the delivery, its pre-delivery
`disk` measurement, and `retention.nodes.<node>`. Each node receipt lists
`removed` images (`id`, `references`, `size_bytes`) and measured
`free_before_bytes`, `free_after_bytes`, `freed_bytes`; the retention root sums
measured freed bytes. Image sizes share layers and must not be summed as actual
reclaimed storage. Filesystem deltas can include concurrent host activity, and
containerd GC may free storage later. A cleanup failure retains partial receipts
and `error`; an unavailable post-error measurement is `null`, not zero.

Node-local history (`/var/lib/sdo-image-retention.json`) protects current and
previous image identities per exact SDO build repository, including separate
role deliveries and repeated deliveries. The first delivery preserves all
pre-existing identities as its unknown prior generation; the next changed
identity establishes the normal two-generation window. Containers (including
exited containers) and images with any foreign alias are protected. App and
third-party deliveries do not rotate SDO history. Cleanup never prunes the host
Docker store, build cache, or registry storage. The configurable
`LaneResourcePolicy.host_disk_floor_bytes` (100 GiB default; delivery CLI
`--min-free-gib`) rejects low disk before either registry delivery or fallback.
