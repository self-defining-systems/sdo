# Fastloop result evidence

Fastloop runs write `results/<run-id>/incidents.jsonl` and `incidents.csv`, one row per injected stream entry. `problem_id` joins explicitly composed IDs with `+`. Use `oracle.success`, `recovery_oracle.success`, and `error` together: a failed incident oracle or failed post-recovery oracle is a failed incident. `injection_started_at`, `detected_at`, `responder_started_at`, `responder_finished_at`, `mitigation_applied_at`, `oracle_finished_at`, `reflection_started_at`, `reflection_finished_at`, `rebuild_done_at`, and `recovery_finished_at` are timestamp columns; `responder_tokens`, `drift_tokens`, and `reflection_tokens` are separate objects in JSONL and JSON strings in CSV. New `run_metadata.json` declares `token_accounting_version: responder-drift/v2`: `drift_tokens` is legacy zero and merged reasoning is charged once to `responder_tokens`. Older rows retain recorded investigator costs; the historical reader sums completed investigations between injection start and resolution end. `diagnosis_grade` is separate from the deterministic oracle: when enabled, it contains SREGym's diagnosis-judge success, accuracy, D1/D2/D3 details, judge tokens, latency, and any judge error. Explicit composites receive one whole-set diagnosis grade. Deferred grades are merged into both result files at run end and never gate the next injection; inline grades run before recovery. With deferred grading, fastloop time-to-mitigation is `injection_to_mitigation_seconds` directly; inline grading occurs after responder resolution and before recovery, so it is excluded from that metric too. Reflection timestamps are the harness-observed drain interval; newer native broker ledgers separately expose closure phase instants. Do not mix these fastloop rows into pipeline stage cost calculations without accounting for that different timing scope.

New native broker ledgers expose `closure_phase_timestamps` as UTC instants and `reflection_skipped_reason`; fastloop rows expose `closure_phase_seconds`, the elapsed seconds since each preceding observed closure phase. Missing phases mean that path did not execute or that the run predates instrumentation. The immutable outcome document is committed before reflection, so join later phases from the broker ledger using the incident ID and outcome commit.

Native `OutcomeRecord.resolution_path` is `fast_path` or `responder`; `fast_path_programs` lists the programs attempted, including a failed attempt handed to the responder. `plan_applications` records each plan's verification plus `rolled_back` and `rollback_error`, and `program_failure_signals` breaks a failed program's trust streak. A fully resolved fast path has no responder Job and zero responder usage. Current closures still settle per-cause learning evidence; older versions skipped reflection for fast paths. The strict native receipt accepts an empty `responder_job_evidence` only for that verified outcome path. Fastloop rows carry `resolution_path` and `fast_path_programs`; composite rows use `fast_path` only when every component receipt used it and combine distinct program IDs. Older outcomes and rows omit these fields.

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
changes eligibility, not the evidence required. The broker persists
`reflection_eligibility` version 1 before committing the authoritative outcome.
Its cause groups exactly preserve `confirmed_root_causes` resources; observations
outside those groups remain unassigned. Only `covered` items for the same program
revision and named bindings supply success credit. They require affirmative
`final_equivalence_verified`, a successful application with bound post-apply
NoMatch, and an explicit NoMatch on the independently verified fixed capture.
Missing proof, unavailable captures, different-field repairs and unbound writes
remain questions, with no success or failure judgment. A definite fixed Match
is a false match: Go records a negative case, revokes trust for that revision,
and gives no credit, even if reflection is rejected.

Corpus publication and reflection dispatch consume that same settled result.
Exact positive and negative cases use ordinary broker validation; the model is
needed for questions or rejected cases. Old repair-text and trusted-program
whole-incident skip shortcuts no longer exist. Forced model reflection retains
all typed work and any Go-authored files.

At most one success per program is credited per incident. Program revisions
fingerprint executable Git objects and imported helpers, excluding Markdown and
Go tests. A new revision starts a new streak; explicit execution failures and
false matches retain suspicion for their revision. Historical outcomes without
settled eligibility earn no new success credit and are not rewritten. Questions
do not add credit or manufacture failures. Inspect the accepted outcome and
broker reflection ledger separately; publication failure cannot change the
already committed eligibility facts.

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
`apply` does not roll back: the signal marks an unverified application that stays applied until the responder issues `rollback` or retries. The tool state saves it so it survives a restart. Trust
counts the signal with the failed application once, or on its own after restart
discards application history; a successful retry in that incident leaves zero
successes and one failure for the affected revision.

### Historical learning credit

Earlier dispatchers could synthesize one origin credit from an accepted program's
introducing corpus replay. Current dispatchers derive credit only from persisted
covered instances. Newly authored or unmatched programs must earn their first
credit on a later exact covered repair; corpus acceptance alone creates no credit.
Do not reinterpret archived counters under the new policy.

After a controller restart, fast-path closure requires fresh bound NoMatch
results plus independent verification. Markers never replay a cached success or
undo a prior write. A read-only recovery closure has no new applied-plan receipt
and earns no additional program trust credit. `PlanApplication` keeps applied
actions and verification outcomes; rollback pre-images and patches are retired
(reserved fields 11 and 13). The tools-state ConfigMap retains attempt counts,
limits and revision-bound failure signals, not mutation history.
