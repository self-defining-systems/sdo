# Native incident restart evidence

### Open incidents after controller restart

The controller is the only application writer. Within a process it applies a
plan, reports independent verification and leaves rollback to the responder (a partial multi-write failure is compensated). A restart
never replays or compensates a stored application, rollback or pending write.

`<IncidentJobName(incident_id)>-tools-state` still uses the CAS state store.
Its `runtime-state.json` is canonical `ControllerState` ProtoJSON;
`responder_tool_state_json` (field 35) is base64-encoded version-2 JSON with
`IncidentID`, `SourceCommit`, `Batches`, `MaxBatches` and `FailureSignals`.
These are attempt limits and revision-bound outcomes. Prepared plans, application
handles, patches, pre-images, rollback progress and tool histories are not saved.
Version-1 state contributes only identity, budgets and failure signals; its old
mutation records are ignored. A restart cannot restore an old handle or increase
the saved response budget. The responder continues or redispatches against live
state and prepares a fresh plan when needed.

Failed responder verification adds
`application_verification_failure:<program_id>@<accepted_dependency_revision>` to
`FailureSignals` and saves it; `apply` does not roll back. It survives any later
responder-issued rollback and restarts without retaining executable application state. A successful
retry does not erase the earlier revision's failure or restore its trust.

Incident-open `playbook_coverage` survives prepared-task replay and is copied unchanged into the outcome. No automatic apply or fast-path restart marker is used. Native receipts require responder Job evidence.

Outcome `PlanApplication` fields 11 (`pre_images`) and 13 (`patches`) are reserved.
Concrete `actions_applied`, verification and timing remain durable evidence.
Controller-state and broker-ledger readers discard only those retired fields
from prior builds; other unknown fields remain errors.
Rollback UID/resourceVersion guards and the refusal of reordered named lists
remain in process; there is no durable rollback owner or cursor.

Sources: `controller_state.proto`, `program.proto`,
`runtime/kubernetes_job_dispatcher_state.go`, `runtime/responder_loop_state.go`,
`runtime/plan_executor{,_receipt}.go` and `runtime/playbook_coverage.go`. For accepted success/failure evidence, see
[program trust evidence](fastloop-results.md#program-trust-evidence).

Foreign NetworkPolicy drift uses the existing typed DriftEvent evidence: `source` states absence from accepted source and the `spec` field records create/edit values. Admission requires a confirmed source-absence read and reviewed kind, not a failed source lookup. Current repeats require findings from independent incident detectors; accepted-source drift remains `drift.unexplained`, and source verification checks policy deletion. Historical `drift.networkpolicy-service-isolation` entries came from the retired proactive path. Normal controller churn remains excluded.

Current controllers have no namespace sweep or program confirmation timer. Retired `program_sweep_cursors` are discarded at durable-state decoding; synthetic program tracker and batch entries are retired on restore, while registered detectors and accepted-source drift are preserved.

Primitive recordings now carry optional `phase` metadata (`open`, `fixed`, or `healthy`); omitted phases mean `open` only. Generalization and rename replay select the matching phase instead of reusing open answers. Since the captured healthy snapshot equals fixed, healthy replay uses fixed observations unless explicit healthy observations exist. The responder source tool records `DiffAgainstSource` through the same live read view, including source absence, even before any program exists. At verified closure the controller repeats inspected primitives against the captured fixed snapshot and appends actual `fixed` observations to each protected open recordings file. Missing later observations yield Unknown, never invented healthy answers.

Program revision identity includes the accepted program directory and its transitive shared-library import closure. Unrelated libraries do not reset trust; changed imported helper sources do.

Mitigator validation is best-effort for external effects. Diagnosis open/fixed,
healthy-peer, generalization and rename replay remain mandatory. SetField-only
plans must repair the open fixture to NoMatch; `program.mitigation-not-repaired`
rejects a retained match, and unavailable post-plan observations remain
`program.mitigation-unreplayable`. A plan containing any non-SetField effect passes
plan validation with `skipped_unreplayable_effect`, without post-plan replay.
`MemoryCheckReport.mitigation_replay_notes` carries typed `MitigationReplayNote`
statuses `STATUS_REPLAYED` or `STATUS_SKIPPED_UNREPLAYABLE_EFFECT`, program/case IDs
and effect types separately from rejecting diagnostics. Notes prove no external effect or trust.

Corpus acceptance supplies no origin credit; current trust consumes only persisted covered instances. Verified successes and revision-attributed failures are informational counters. Equivalent
ad-hoc repairs retain bound plan-equivalence receipts and post-repair NoMatch;
unrelated repairs in the outcome do not suppress credit. Every planned resource
needs a successful receipt, exact observed plan equivalence, and independent
outcome success. Match-to-NoMatch alone earns nothing. Summaries distinguish verification and mutation. Each program earns at most one
success per outcome; failed applications retain their writes for the responder and increment failure evidence.

Source-comparison recordings preserve the accepted manifest fields and the full observed fields in `result.text` as `accepted: <JSON>\nobserved: <JSON>`, even when `changed` is false. Missing objects use `null`. The accepted side never contains live fields retained by an apply dry run. Compare relevant fields directly, including absence: `changed` still describes reconciliation against server-side-apply normalization (or declared-field projection offline), so it can be false for a live-only field owned by another manager. Source revision and kubectl last-applied bookkeeping annotations are excluded. Existing archived recordings are unchanged.


Responder continuation retains the same incident, proposal checkout, provider session and original deadline. The native dispatcher independently verifies each returned response and persists a bounded continuation before creating its distinct immutable task/Job. Controller verification can resume a dispatch that initially looked healthy. A failed model return is not terminal while continuation time/loop budget remains. Inspect responder tool state Continuation, per-task checkpoints and controller verification evidence; sum incremental per-turn usage, deduplicate repair action IDs, and do not count continued jobs as separate incidents. Optional usage totals remain unknown if any component is unknown. ResponseLoopsUsed includes returned response attempts as well as the maximum observed apply-batch count.


Controller verification also emits `controller.rollout/deployment-rollout-pending` findings for incomplete Deployment generations and active old-template Pods related to current findings or recorded writes in the incident namespace, including recorded-action repairs without a plan application. Evidence joins Deployment → ReplicaSet → Pod by owner UID and names ReplicaSet failures, Pod scheduling/container failures and owned Warning events. These findings use the existing `findings` and continuation `still_broken` fields; an accepted template write or old Ready replica does not establish recovery. Service selectors and owner identities preserve the incident scope; helper workloads, unrelated Deployments and other namespaces are excluded.


Unconfirmed verification after a completed responder is controller-owned work. The dispatcher returns promptly rather than polling observation windows. `ControllerState.verification_deadline_at` freezes the first completed response’s verification bound across continuations and controller restart; it never exceeds the original response deadline. `Continuation.UnconfirmedPrompted` in the responder tool journal admits one resumed turn through `ContinueAfterVerification`; it survives restart and shares the original request deadline and loop budget. Feedback and `sdo verify` expose `verification_blockers: [{detector_id, reason}]`; the bounded Python prompt retains complete JSON evidence on disk. With no actionable findings at the verification bound, the controller begins a normal receipted `unconfirmed` closure, never `OwnerLost`. The closure's `unconfirmed_detail` names incomplete observations, and its partial pending detector evaluations reach `OutcomeRecord.detector_history` with detector IDs and reasons. Active findings still close failed. A live controller recovering a harvested result owns closure even during maintenance.

On a closure failure or missing receipt, the benchmark preserves `sdo-controller-state.json` in the row's artifact directory and controller logs under `sdo_runtime/controller_logs/` before any subsequent lane wipe. Export attempts are independent: an unreadable state does not suppress logs or replace the original error. Deferred receipt publication copies the state snapshot too. These are eval diagnostics, never production verification inputs.
