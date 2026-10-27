# Fastloop result evidence

`fastloop.json` and every new incident row carry `image_delivery` (`registry`, `kind-load-fallback`, or `unknown` for older lanes) and `image_delivery_reason`. Surface fallback and its registry error in reports. Completion rows include mid-run source-deployment fallbacks. `image_delivery.jsonl` records each attempted delivery's cluster, image list, path, reason and wall seconds; the lane additionally retains `image_delivery_seconds` and `image_delivery_images`. Successful registry delivery of every affected image clears fallback mode; a partial success cannot hide another image's fallback. Code resets preserve delivery mode without transferring images. Fallback warns on stderr when delivery fails, after the archive transfer, and at each run startup. Delivery metadata is benchmark transport evidence.

Fastloop runs write `results/<run-id>/incidents.jsonl` and `incidents.csv`, one row per attempted stream entry. `problem_id` joins explicitly composed IDs with `+`. Use `oracle.success`, `recovery_oracle.success`, and `error` together: a failed incident oracle or failed post-recovery oracle is a failed incident. `injection_started_at`, `detected_at`, `responder_started_at`, `responder_finished_at`, `mitigation_applied_at`, `oracle_finished_at`, `reflection_started_at`, `reflection_finished_at`, `rebuild_done_at`, and `recovery_finished_at` are timestamp columns; `responder_tokens`, `drift_tokens`, and `reflection_tokens` are separate objects in JSONL and JSON strings in CSV. New `run_metadata.json` declares `token_accounting_version: responder-drift/v2`: `drift_tokens` is legacy zero and merged reasoning is charged once to `responder_tokens`. Older rows retain recorded investigator costs; the historical reader sums completed investigations between injection start and resolution end. `diagnosis_grade` is separate from the deterministic oracle: when enabled, it contains SREGym's diagnosis-judge success, accuracy, D1/D2/D3 details, judge tokens, latency, and any judge error. Explicit composites receive one whole-set diagnosis grade. Deferred grades are merged into both result files at run end and never gate the next injection; inline grades run before the reflection drain and recovery. With deferred grading, fastloop time-to-mitigation is `injection_to_mitigation_seconds` directly; inline grading occurs after responder resolution and before recovery, so it is excluded from that metric too. Reflection timestamps are the harness-observed drain interval; newer native broker ledgers separately expose closure phase instants. Do not mix these fastloop rows into pipeline stage cost calculations without accounting for that different timing scope.

For injection occurrence, grade checkpoints and per-fault probe semantics, read [fault tracking evidence](fault-tracking.md).


New native broker ledgers expose `closure_phase_timestamps` as UTC instants and `reflection_skipped_reason`; fastloop rows expose `closure_phase_seconds`, the elapsed seconds since each preceding observed closure phase. Missing phases mean that path did not execute or that the run predates instrumentation. The immutable outcome document is committed before reflection, so join later phases from the broker ledger using the incident ID and outcome commit.

Native `OutcomeRecord.resolution_path` is `responder` when populated. `playbook_coverage` records incident-open matches and unexplained findings; rows retain it even if receipt validation fails. Every current native receipt requires one successful responder Job. Program selections, raw agent actions, final verification and explicit rollbacks are recorded separately. See [agent-selected repair evidence](automatic-repair.md).

Snapshot captures may include `statefulSets`, `horizontalPodAutoscalers`, and `podDisruptionBudgets` alongside Deployments, Services, ReplicaSets, and Pods. Use their same-namespace selectors, scale targets, and controller owner references to audit relation coverage; a matching name alone is insufficient.

Composite fastloop rows sum each `closure_phase_seconds` duration across their strict incident receipts and combine distinct reflection skip reasons. Strict receipts also record `reflection_drain.phase_timestamps` (`drain_started`, `ack_observed`, `receipt_collected`) and `reflection_drain.phase_seconds` (`wait_for_ack`, `collect_settled_receipt`). The collection phase includes Go evidence retrieval and accepted repository sync. A long collection phase after `ack_observed` is transport cost, not reflection execution.

Fastloop rows also expose `run_prelude_phase_seconds` on the first incident only (`worker_start`, `worker_deploy`, `fault_description`, `baseline_and_driver_setup`, and `total_before_first_incident`), plus `driver_phase_timings_seconds` and `fault_gate_timings_seconds` from the persistent-stage resolution. The driver map separates `controller_inventory`, `lifecycle`, `controller_install_inspection`, `controller_install_or_reuse`, `fault_gate`, and later recovery phases. The fault gate map separates the fresh baseline evaluation wait from the injection request. `fault_description` may overlap `worker_deploy`, so their sum need not equal the prelude total. An inspection skipped after a fresh lifecycle install reports `controller_install_inspection=0`. Reused stages run the Go inspection to restore application-namespace grants after namespace recreation; its duration is included even when the lifecycle fingerprint and live Pod UID match. Older runs skipped this required restoration and may report zero on reuse; older rows can also lack these maps even when their `sdo_incident_resolution.json` has them.

Rows also count program diagnoses and verdicts, tool calls and commands, and response loops. On a permanently failed closure, the fastloop retains recoverable timing, usage, and program evidence from the controller's pending closure, records the closure error, and stops the stream. A missing diagnosis has `diagnosis_grade.skipped: true` with a reason; it is never sent to the diagnosis judge.

Benchmark fault recovery has a separate intake fence: SDO must have no pending
closure, prove the expected incident durably acknowledged (or terminally failed),
request maintenance pause, observe its exact paused-generation acknowledgment,
and recheck that no incident raced admission. Missing state, missing pause support,
unclosed incidents or failed acknowledgment stop recovery before any harness write.
Normal cleanup, invalid-injection cleanup, interruption and repeated cleanup passes
all use this fence. The persistent conductor driver also drains terminal bookkeeping
and establishes the same fence before signaling application teardown. Intake remains paused through recovery verification; the next
stage resumes it only through the existing baseline/injection gate. A blocked
recovery is an eval failure, not an SDO repair or a repeat encounter.

## Finding-triggered playbook diagnosis

Current controllers run incident detectors continuously. Playbook Diagnose runs only while handling detector findings, with one binding per finding/program; watch events and timers never enumerate playbook roles. Accepted-source drift remains `drift.unexplained`. Program diagnoses and matches belong to incident coverage, responder commands, and self-check evidence.

Historical runs can contain `drift.<program>` findings and `DriftEvent.source` values `watch event program match` or `periodic program sweep`. Those were proactive playbook detections, with their own firing/clearing confirmation timers; do not interpret them as current detector-driven coverage. Current controller state has no program sweep cursor, and the former sweep/persistence flags have been removed.

Reflection's shared memory check runs recorded faulty observations through incident detectors, production finding-to-program selection, Match diagnosis, and mitigation replay on that same binding. A program or detector defect on that path is a blocking rejection under its own rule: `program.diagnose-no-match` (a selecting finding binds an object Diagnose does not match), `program.mitigator-missing`, `detector.invalid-finding`, `detector.fires-on-healthy` (an incident detector fires on the recorded healthy fixture), `program.repeat-unavailable` (a recorded source or detector read failed). `program.repeat-unreachable` is only a coverage-gap note: no finding exists or none selects the playbook. The mitigation is still replayed with the case bindings and must repair (`program.mitigation-not-repaired`). Case-file object bindings do not select the repeat-path diagnosis. Coverage gaps are counted from the incident ledger's `reflection_coverage_gaps` (see `native-reflection.md`).

## Program counts and settlement evidence

`runtime/program_counts.go` tallies per-(program id, revision) success/failure counts from accepted outcomes: an applied, non-dry-run application succeeds when its incident closed healthy and it was not rolled back, and fails otherwise or on a recorded `application_verification_failure`. The counts are shown to the responder and gate nothing. Unreadable outcomes are skipped and logged once. Reflection settlement is separate: the broker persists
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
Go tests. Broker-validated revisions preserve the success streak; explicit execution failures and
false matches retain suspicion for their revision. Historical versions reset on edits. Historical outcomes without
settled eligibility earn no new success credit and are not rewritten. Questions
do not add credit or manufacture failures. Inspect the accepted outcome and
broker reflection ledger separately; publication failure cannot change the
already committed eligibility facts.

Program failures are recorded as `application_verification_failure:<program_id>@<accepted_dependency_revision>`; counters describe evidence and never gate dispatch.

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

After a controller restart, the original coverage remains in the prepared incident task. The responder rechecks live state before choosing repairs. `PlanApplication` keeps applied
actions and verification outcomes; rollback pre-images and patches are retired
(reserved fields 11 and 13). The tools-state ConfigMap retains attempt counts,
limits and revision-bound failure signals, not mutation history.


For capability policy, infrastructure outcomes, mixed-app ordering and image delivery evidence, read [fastloop dispatch evidence](fastloop-dispatch.md).
## Seed recovery oracle

A full seed-compatible cleanup intentionally removes fault-created objects. Its `recovery_oracle` is an independent baseline check plus a fresh HTTP probe, not another mitigation verdict for the removed injection. Keep the original `oracle` and its failed components unchanged. A failed or incomplete seed probe blocks reuse and keeps `full_cleanup_verified=False`; no benchmark verdict is supplied to production learning. Legacy drivers without this check retain their fault-oracle recovery behavior.

### Startup versus resumed baseline timing

The persistent fault gate keeps controller startup (including health refresh) separate from its 300-second resumed-baseline readiness budget. The matching maintenance `active` record starts that readiness clock once; later polls or pauses cannot restart it. Startup remains bounded by the existing lifecycle/startup budget. `controller_baseline_wait` retains the total pre-injection wait; `controller_startup_wait` and `controller_resumed_baseline_wait` split it. None belongs to incident TTM or per-incident responder/reflection usage.

Warm resets require structural health and independent HTTP. Failed HTTP permits one bounded hotel-client refresh and recheck, preserving backends/memory. Failed refresh or HTTP blocks admission; restoration earns no responder credit.

### Protected healthy references in offline checkpoints

Checkpoint capture maps retain the controller's frozen `healthy/healthy.json` and
`healthy/reference` alongside ordinary incident capture files. These are the only
nested capture names; traversal and other nested paths are rejected. Export and
both worker/broker replay restore the same private directory and retain existing
per-file, bundle and content-digest checks. A missing historical healthy reference
is a replay evidence gap for a closure that did not verify healthy; never substitute an agent-authored corpus healthy state.
On a verified-healthy closure with no pre-incident reference, the controller binds its own `fixed` capture as `healthy/healthy.json` (`reference` is `closure`). A session-authored positive replay case can still
omit incident captures when its controller healthy reference is retained.

## Concurrent shared-memory lanes

`fastloop parallel` joins ordinary per-lane rows into output `incidents.jsonl`
and CSV. `parallel_plan.json` records encounter order; `parallel_result.json`
reports requested/completed counts and every lane error. Completed peer evidence
survives a lane failure. Aggregate `index` follows input order; `lane_id` names
the private controller/cluster. Original rows and immutable grade checkpoints
retain lane-local indices. Never double-count copies. Missing encounters and
duplicate incident identities fail the aggregate.

`memory_before_commit`, `memory_after_commit` and `memory_ancestry` bound an
encounter's accepted state. Ancestry includes every intervening peer publication
in Git ancestry order. Bounds precede admission and follow closure grading,
reflection drain and fenced recovery; they describe observed state, not causal
learning credit. Join native receipt request/outcome/reflection commits for exact
incident inputs. A commit rewind is an eval error.

Lanes overlap but each lane stays sequential, preserving grade-at-closure and
paused recovery. Oracle values never enter memory. Shared mode rejects seeding,
archive copy-back and reset. The external attachment report records phases and
original Jobs. Attachment requires equal initial accepted trees and retains
private proposal, ledger, capture and session roots. Scripted scheduling, broker
and lock tests prove plumbing; live runs establish learning and speedup.

Install-time pending namespace inventory records pre-existing environment UID/configuration/ownership digests independently of healthy onboarding. A rejected health window preserves only matching captured observations; replacement or changed objects remain unexplained presence drift. Check the immutable pending snapshot and install boundary; source-declared faults, health/reachability checks and objects added after that boundary remain active. This rule does not label, delete or recreate environment objects.
For private lane journals and warm-app conversion receipts, see
[restartable lanes and application conversion](fastloop-sequences.md).
