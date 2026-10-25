# Native incident restart evidence

### Open incidents after controller restart

The controller is the only application writer. Within a process it applies a
plan, reports independent verification and leaves rollback to the responder (the fast path rolls back its own failed attempt; a partial multi-write failure is compensated). A restart
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

`<IncidentJobName(incident_id)>-fast-path` stores `state` (`attempting`, `failed`,
`resolved`), bound `program_runs`, revision-bound `trust_failure_signals`, and a
failure `summary`. The controller freezes the diagnoses and failure suspicion
before writing, and consumes its one fast-path attempt durably. After restart,
every original binding must diagnose NoMatch at the same program revision and
pass fresh detector and independent health verification to close. A remaining
Match, Unknown, NeedsJudgement, missing program, changed revision or read error
hands the incident to the responder. The existing one-attempt limit prevents a
second fast-path write; the remaining responder budget still applies.

A resolved marker is an observation, never cached authority to close. Failed
attempts retain their original revision-bound signals even if the cluster later
recovers. Suspicion frozen for an interrupted attempt is restored when live
rechecking cannot establish a verified fix. Legacy markers without bound diagnoses
escalate; their `application`, `preimages`, `tool_state` and `result` are ignored.
Do not infer a repair receipt or new trust credit from a restart's read-only
closure: it has fresh diagnosis and verification, but no application receipt.

Outcome `PlanApplication` fields 11 (`pre_images`) and 13 (`patches`) are reserved.
Concrete `actions_applied`, verification and timing remain durable evidence.
Controller-state and broker-ledger readers discard only those retired fields
from prior builds; other unknown fields remain errors.
Rollback UID/resourceVersion guards and the refusal of reordered named lists
remain in process; there is no durable rollback owner or cursor.

Sources: `controller_state.proto`, `program.proto`,
`runtime/kubernetes_job_dispatcher_state.go`, `runtime/responder_loop_state.go`,
`runtime/plan_executor{,_receipt}.go`, `runtime/program_fast_path_recheck.go` and
`runtime/kubernetes_fast_path.go`. For accepted success/failure evidence, see
[program trust evidence](fastloop-results.md#program-trust-evidence).

Foreign NetworkPolicy drift uses the existing typed DriftEvent evidence: `source` states absence from accepted source and the `spec` field records create/edit values. Admission requires a confirmed source-absence read and reviewed kind, not a failed source lookup. A learned repeat is `drift.networkpolicy-service-isolation`; policy deletion triggers replay on the retained binding and requires explicit NoMatch before clearing. Normal controller churn remains excluded.

Namespace sweeps persist `program_sweep_cursors` in controller state as the next mixed-radix assignment per program. Stable binding sets rotate through the complete required and optional parameter product with at most 64 new diagnoses per program per sweep; optional parameters may remain unbound. A controller restart preserves this progress.

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

Corpus acceptance supplies no origin credit; current trust consumes only persisted covered instances. Default trust requires three independently
verified live successes and zero failures for the unchanged revision. Equivalent
ad-hoc repairs retain bound plan-equivalence receipts and post-repair NoMatch;
unrelated repairs in the outcome do not suppress credit. Every planned resource
needs a successful receipt, exact observed plan equivalence, and independent
outcome success. Match-to-NoMatch alone earns nothing. Summaries distinguish verification and mutation. Each program earns at most one
success per outcome; failed live fast paths roll back and block revision trust.

Source-comparison recordings preserve the accepted manifest fields and the full observed fields in `result.text` as `accepted: <JSON>\nobserved: <JSON>`, even when `changed` is false. Missing objects use `null`. The accepted side never contains live fields retained by an apply dry run. Compare relevant fields directly, including absence: `changed` still describes reconciliation against server-side-apply normalization (or declared-field projection offline), so it can be false for a live-only field owned by another manager. Source revision and kubectl last-applied bookkeeping annotations are excluded. Existing archived recordings are unchanged.


Responder continuation retains the same incident, proposal checkout, provider session and original deadline. The native dispatcher independently verifies each returned response and persists a bounded continuation before creating its distinct immutable task/Job. Controller verification can resume a dispatch that initially looked healthy. A failed model return is not terminal while continuation time/loop budget remains. Inspect responder tool state Continuation, per-task checkpoints and controller verification evidence; sum incremental per-turn usage, deduplicate repair action IDs, and do not count continued jobs as separate incidents. Optional usage totals remain unknown if any component is unknown. ResponseLoopsUsed includes returned response attempts as well as the maximum observed apply-batch count.

