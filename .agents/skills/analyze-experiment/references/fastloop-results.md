# Fastloop result evidence

`fastloop.json` and every new incident row carry `image_delivery` (`registry`, `kind-load-fallback`, or `unknown` for older lanes) and `image_delivery_reason`. Surface fallback and its registry error in reports. `image_delivery.jsonl` records each attempted delivery's cluster, image list, path, reason and wall seconds; the lane additionally retains `image_delivery_seconds` and `image_delivery_images`. Successful registry delivery of every affected image clears fallback mode; a partial success cannot hide another image's fallback. Code-only resets retain the last mode and never transfer images. Fallback warns on stderr when delivery fails, after the archive transfer, and at each run startup. These are benchmark transport facts, not production outcome evidence.

Fastloop runs write `results/<run-id>/incidents.jsonl` and `incidents.csv`, one row per injected stream entry. `problem_id` joins explicitly composed IDs with `+`. Use `oracle.success`, `recovery_oracle.success`, and `error` together: a failed incident oracle or failed post-recovery oracle is a failed incident. `injection_started_at`, `detected_at`, `responder_started_at`, `responder_finished_at`, `mitigation_applied_at`, `oracle_finished_at`, `reflection_started_at`, `reflection_finished_at`, `rebuild_done_at`, and `recovery_finished_at` are timestamp columns; `responder_tokens`, `drift_tokens`, and `reflection_tokens` are separate objects in JSONL and JSON strings in CSV. New `run_metadata.json` declares `token_accounting_version: responder-drift/v2`: `drift_tokens` is legacy zero and merged reasoning is charged once to `responder_tokens`. Older rows retain recorded investigator costs; the historical reader sums completed investigations between injection start and resolution end. `diagnosis_grade` is separate from the deterministic oracle: when enabled, it contains SREGym's diagnosis-judge success, accuracy, D1/D2/D3 details, judge tokens, latency, and any judge error. Explicit composites receive one whole-set diagnosis grade. Deferred grades are merged into both result files at run end and never gate the next injection; inline grades run before recovery. With deferred grading, fastloop time-to-mitigation is `injection_to_mitigation_seconds` directly; inline grading occurs after responder resolution and before recovery, so it is excluded from that metric too. Reflection timestamps are the harness-observed drain interval; newer native broker ledgers separately expose closure phase instants. Do not mix these fastloop rows into pipeline stage cost calculations without accounting for that different timing scope.

New native broker ledgers expose `closure_phase_timestamps` as UTC instants and `reflection_skipped_reason`; fastloop rows expose `closure_phase_seconds`, the elapsed seconds since each preceding observed closure phase. Missing phases mean that path did not execute or that the run predates instrumentation. The immutable outcome document is committed before reflection, so join later phases from the broker ledger using the incident ID and outcome commit.

Native `OutcomeRecord.resolution_path` is `fast_path` or `responder`; `fast_path_programs` lists the programs attempted, including a failed attempt handed to the responder. `plan_applications` records each plan's verification plus `rolled_back` and `rollback_error`, and `program_failure_signals` breaks a failed program's trust streak. A fully resolved fast path has `reflection_skipped_reason: trusted program fast path`, no responder Job, and zero responder usage. The strict native receipt accepts an empty `responder_job_evidence` only for that verified outcome path. Fastloop rows carry `resolution_path` and `fast_path_programs`; composite rows use `fast_path` only when every component receipt used it and combine distinct program IDs. Older outcomes and rows omit these fields.

When a configured fast path does not attempt a plan, `fast_path_skipped_reason` records the typed eligibility gate in the native ledger, outcome, and fastloop row. The controller log prints the same reason. `uncovered_finding` means at least one active finding had neither its own trusted match nor a direct snapshot relation to a selected trusted run. `fast_path_relation_covered_findings` is a list of finding fingerprints paired with the covering `program_id` and its named `bindings`; it appears in the fast-path result, native ledger, outcome, and fastloop row. A relation-covered finding must clear during verification or the attempt rolls back, hands off to the responder, and records `fast_path_failure` for the program. Fastloop `match_reasons` includes program verdict evidence facts and `binding_sources` lists the sources of matching runs (`hint`, `finding`, or `sweep`); duplicate reasons and sources are collapsed in each row. Older records omit these fields.

Snapshot captures may include `statefulSets`, `horizontalPodAutoscalers`, and `podDisruptionBudgets` alongside Deployments, Services, ReplicaSets, and Pods. Use their same-namespace selectors, scale targets, and controller owner references to audit relation coverage; a matching name alone is insufficient.

Composite fastloop rows sum each `closure_phase_seconds` duration across their strict incident receipts and combine distinct reflection skip reasons. Strict receipts also record `reflection_drain.phase_timestamps` (`drain_started`, `ack_observed`, `receipt_collected`) and `reflection_drain.phase_seconds` (`wait_for_ack`, `collect_settled_receipt`). The collection phase includes Go evidence retrieval and accepted repository sync. A long collection phase after `ack_observed` is transport cost, not reflection execution.

Fastloop rows also expose `run_prelude_phase_seconds` on the first incident only (`worker_start`, `worker_deploy`, `fault_description`, `baseline_and_driver_setup`, and `total_before_first_incident`), plus `driver_phase_timings_seconds` and `fault_gate_timings_seconds` from the persistent-stage resolution. The driver map separates `controller_inventory`, `lifecycle`, `controller_install_inspection`, `controller_install_or_reuse`, `fault_gate`, and later recovery phases. The fault gate map separates the fresh baseline evaluation wait from the injection request. `fault_description` may overlap `worker_deploy`, so their sum need not equal the prelude total. An inspection skipped after a fresh lifecycle install reports `controller_install_inspection=0`. Reused stages run the Go inspection to restore application-namespace grants after namespace recreation; its duration is included even when the lifecycle fingerprint and live Pod UID match. Older runs skipped this required restoration and may report zero on reuse; older rows can also lack these maps even when their `sdo_incident_resolution.json` has them.

Rows also count program diagnoses and verdicts, tool calls and commands, and response loops. On a permanently failed closure, the fastloop retains recoverable timing, usage, and program evidence from the controller's pending closure, records the closure error, and stops the stream. A missing diagnosis has `diagnosis_grade.skipped: true` with a reason; it is never sent to the diagnosis judge.

## Learned-program watch confirmations

Learned `drift.<program>` findings evaluate relevant non-initial watch changes
without ownership or accepted-source admission gates. The attributed
`drift.unexplained` path keeps its original field ownership rules. Watch matches
retain `source="watch event program match"` (or the owned field event evidence)
and hint bindings; fallback discoveries retain `source="periodic program sweep"`.

Events, periodic sweeps and scheduled per-binding confirmations share the finding
tracker. The default program policy requires 10 seconds of sustained Match,
confirmed by a fresh diagnosis at or after the deadline, before activation;
clearing is confirmed symmetrically. `--program-min-duration` configures this
duration. Controller-state `finding_states` retain `first_seen_at`,
`first_clear_at`, and `last_observed_at` so pending confirmations survive restart.
Use the activation/detection timestamp for time to detection; a drift event's
`observed_at` records the initial observation and can precede activation.
Brief rollout mismatches should appear only as pending or `not_persisted`
observations; rollouts that remain mismatched beyond the configured duration can
still activate, regardless of which actor changed the object.

For a confirmed watch-time program match, `DriftEvent.source` is `watch event program match` for both owned and unowned objects. Owned-field diffs, manager/operation attribution and original watch timestamps remain attached as evidence. `differs from accepted manifest` remains the ordinary attributed path when no learned program matches. Older owned-object matches can carry that ordinary source string even though their detector and hint binding identify the learned program; do not confuse those historical labels with periodic sweep discovery.

## Program trust evidence

`runtime/program_trust.go` derives trust from accepted outcomes in completion order.
The default threshold is three verified successes; `--program-trust-threshold=1`
changes eligibility, not the evidence required. An application earns credit only
when all of the following hold:

- The independently derived outcome classification is `success`, and the
  application was neither a dry run nor rolled back. Application errors,
  rollback errors and program-scoped failure signals disqualify it.
- The application's health verification is `NO_MATCH`. This field alone is a
  health-only verdict and does not prove the program diagnosed the repair.
- Successful controller-recorded `plan` and `apply` tool calls connect the plan
  IDs to the application ID and its selected program and named bindings. The
  apply call is at or after `completed_at`; its response contains explicit
  post-apply `NoMatch` for every selected binding of that program. Compare full
  object refs, including API version, namespace and name, and the accepted
  dependency revision. Missing/truncated history, wrong bindings or revisions,
  diagnosis errors, `Unknown`, `NeedsJudgement` and `Match` earn no credit.

Ordinary unapplied applications are ignored; fast-path records are also inspected
when unapplied so attempted failures can revoke trust. At most one success per
program is credited from an outcome. These evidence rules apply
to ordinary, fast-path and equivalent-repair applications; an application ID
prefix or a flat `program_runs` list supplies no exception. Current fast paths
record controller-authored plan/apply calls. An `equivalent-repair-*` record
without the required bound tool evidence earns no credit. Failed, partial,
cancelled, external-recovery, unclassified and unverified outcomes cannot earn
successes even when an immediate health check passed.

Trust is bound to `program_runs.program_revision`: the digest of accepted program
Git objects and their transitive shared-library imports. A revision change starts
a new success streak. Failed applications reset successes and retain suspicion
for that dependency revision. Missing or inconclusive bound evidence also resets
the success streak. The counters are derived values, exposed by `sdo status`;
they are not a separately authoritative durable trust record.

`program_failure_signals` can contain
`fast_path_failure:<program_id>@<accepted_dependency_revision>`. The fast-path
marker freezes this revision before mutation, then restores the signal on failed
handoff or interrupted recovery. Failures remain associated with that revision
after unrelated source commits, edits and later reversions; later successes do
not erase them. Legacy `fast_path_failure:<program_id>` uses the known program
revision, falling back to the source commit only when none is available. Generic
signals such as `adhoc_coverage_gap` are not a failure of every program. Inspect
the attempt facts and live recheck evidence described in [native broker evidence](native-broker.md)
when an outcome has not yet been committed.

Responder applications record the same revision-bound trust failure as
`application_verification_failure:<program_id>@<accepted_dependency_revision>`.
The tool state saves it before rollback, regardless of rollback's result. Trust
counts the signal with the failed application once, or on its own after restart
discards application history; a successful retry in that incident leaves zero
successes and one failure for the affected revision.

### Learning credit

A newly introduced program may earn one credit from its original independently
successful incident, although it did not perform that repair. The memory reader
joins the immutable outcome by incident ID to captured root-cause labels in the
introducing accepted reflection commit. It requires the broker's incident,
actor, phase and passed-validation trailers, matching open/fixed capture hashes,
and newly introduced diagnosis and mitigation sources. Synthetic cases and later
revalidation of an existing program do not supply new learning credit.

The dispatcher additionally checks that the current accepted program/dependency
revision equals the introducing revision, loads immutable accepted captures and
phase recordings, validates bindings and capture hashes, and executes diagnosis:
open must be `Match`, fixed must be `NoMatch` on the same binding. Inconclusive
replay or an unsuccessful original outcome earns nothing. The dispatcher makes a
transient `learned-repair-*` application and plan/apply-shaped replay receipt for
the trust calculation. It never rewrites the original outcome or publishes those
receipts as live tool calls. Audit the outcome, introducing commit, corpus
provenance and replay together; do not look for a live learned-program application
in the original outcome. Program-scoped failures still invalidate this credit.

After a controller restart, fast-path closure requires fresh bound NoMatch
results plus independent verification. Markers never replay a cached success or
undo a prior write. A read-only recovery closure has no new applied-plan receipt
and earns no additional program trust credit. `PlanApplication` keeps applied
actions and verification outcomes; rollback pre-images and patches are retired
(reserved fields 11 and 13). The tools-state ConfigMap retains attempt counts,
limits and revision-bound failure signals, not mutation history.
