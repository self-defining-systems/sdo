# Native acceptance and memory evidence

## Broker incident ledger

For active Go runs, inspect the configured runtime `LedgerRoot` and the Go broker workspace root. The broker stages trusted source deployment evidence before publishing source, then reconciles that evidence to the attributed accepted commit after a crash. The evidence binds the candidate Git tree, strategy, immutable image digests, resource IDs, and observed ready revision; agent claims and annotations alone are not authoritative.

Ledger files under `.git/sdo-broker/` and the controller's runtime-state ConfigMap are canonical ProtoJSON of `BrokerLedger` and `ControllerState` (`controller_state.proto`): snake_case keys, empty and zero fields omitted (so a missing key means the zero value), RFC3339 timestamps, enums as names, and the closure as an `IncidentClosure`. The Go structs carry no JSON tags, so pre-cutover ledgers with legacy keys (for example `playbooks` or `path` in place of `playbook_ids`) are not readable by the current tools. `reflection_rejections` lists each broker rejection of a reflection proposal (`attempt`, typed `diagnostics` of `rule`/`path`/`line`/`message`/`hint`, `rejected_at`); Go re-dispatches reflection with the previous rejection's diagnostics up to twice, and `reflection_rejected` marks the terminal rejection after the bound (the closure completes with no reflection commit). Ledger fields can include proposal processing state; optional proposal commit; mandatory outcome, reflection, and validator-evidence commits; responder session ID; accepted detector paths; closure and acknowledgement state; network-policy canaries; topology fingerprints; and controller-update rollout records. Current native ledgers use `reflection_rejections`, `reflection_rejected`,
`reflection_processed`, and typed `reflection_decision` (decision, reason, logical
playbook IDs, error, SDK gaps and usage). `ReflectionDecision.playbook_ids` remains
wire field 3. A verified fast path skips reflection; exact accepted repair replay
or wholly trusted program repair may also record a skip reason. A failed learning
turn preserves the committed mitigation and records its error. The closure's `request.state_changes` (absent in older ledgers or without a healthy baseline) is the controller's configuration diff against its last healthy baseline when the incident opened: `baseline_at`, `observed_at`, `changes` (`kind`, `name`, `change` of `added`/`removed`/`modified`, and field-level `before`/`after`, where ConfigMap and Secret data appear only as `sha256:` digests), `omitted`, and `unobserved_kinds`; objects that existed unchanged at the baseline, such as SREGym's `failure-admin-*` decoys, never appear. The closure's `final_state_changes` (absent in ledgers before the verification-time state diff, and without a healthy baseline) is the same shape of diff, recomputed at verification time instead of dispatch time: a composite fault's later component can land a few seconds after dispatch while the incident is still open (K2's readiness-probe fault about 6s after its selector fault), so it can appear here without appearing in `request.state_changes`. `verify_diagnosis` checks `state-change` evidence against the union of both, so a diagnosis citing only the late component is `confirmed`/`unverified` rather than `contradicted`; a change absent from both is still `contradicted`. The closure's `observed_state_changes` (absent before rc2, and without a healthy baseline) lists every object the controller saw differ from the baseline while the incident was open (`kind`, `name`, `first_observed_at`): the dispatch diff, each evaluation's incident-view diff, and the closing view; it is what dates a `state-change` citation against the responder's repair. The closure's `health_cleared_at` (absent in ledgers before repair attribution, and when a gate detector had no clear streak on record) is when the closure gate's detectors began their final streak of clear evaluations; only a repair action that started by then can back a root cause. The closure's `cleaned_helpers` (absent in older ledgers) lists the responder helper pods and Jobs (`sdo.dev/responder-helper=true`) the controller deleted when the responder completed. The closure's `incident_detector_states` (absent in older ledgers) holds the latest post-response evaluation of each non-health detector that raised a finding; it is learning evidence, not a closure gate. The closure's `detector_review_required_at` and `detector_review_reason` (absent in older ledgers and when health cleared in time) mark health that cleared only after the controller's verification window expired; the broker then treats health as unverified for the responder (classification `partial`, no `verified_at`, no reflection), and the receipt's `detector_review_required_at` makes strict validation reject it. Under `recorded-actions`, a missing proposal commit is valid only when the result and outcome contain a successful structured repair action. Require incident IDs and commit hashes to agree with the receipt and outcome.


### Historical Python broker fields

The following fields describe retired Python broker ledgers; do not expect them in current native ProtoJSON.

`reflection_attempts` counts reflection turns, including turns whose backend failed; `reflection_backend_error` (absent in older ledgers) is the last such failure, and once the attempts are spent the broker records `no_change` with that error as the reason instead of failing the closure; `reflection_fresh_retry_attempts` (default 0 in older ledgers) counts the subset that retried a validator-rejected proposal in a fresh session instead of resuming the responder session. `reflection_session_mode` (null in older ledgers and when no LLM reflection ran) records how the first attempt ran: `fresh` (the default: a new session given a bounded incident brief of the closure findings and evidence, the responder's structured result, its shell commands from `responder-turns.jsonl`, and relevant memory excerpts; under about 8K tokens) or `resume` (opt-in: the responder session). It is fixed when the first LLM attempt starts, so a restarted broker keeps it. The rejected proposal's diff is kept beside the ledger as `<sha256(incident_id)>.reflection-rejected.diff`. `reflection_skipped_reason` (null in older ledgers) is set when the broker recorded a deterministic no-op reflection without an LLM turn: a repeated exact-match success where the warm rule held (a prior verified outcome matched by `exact-fingerprint`, or by `learned-detector-origin` when an incident detector learned from that outcome fired on the same resource and surfaced a playbook listed in the `possiblePlaybooks` of a registered responder-owned incident detector, fired or not), the responder applied that playbook, every repair action and verification passed, the controller verified health, and that detector was not firing in its latest post-response evaluation (no post-response evaluation is accepted). The reason names the prior incident(s), each playbook with its detector, whether the detector had fired at dispatch, how the playbook surfaced (`active-finding`, `surfaced`, `prior-outcome-applied`, `detector-learned-from-exact-prior`), and each detector's post-response state (`clear after the response` or `not evaluated after the response`). Such a ledger has `reflection_attempts=0`, empty `reflection_usage`, `reflection_learning_decision=no_change` with the same text as `reflection_no_change_reason`, an empty attributable reflection commit, and no accepted detector paths or controller rollout.
## Operational memory layout (schema version 3)

Detectors are registered by per-detector manifests at `.sdo/diagnostics/detectors/<health|incidents>/<id>/manifest.yaml` (the directory name is the detector ID). The former single `.sdo/diagnostics/manifest.yaml` index exists only in schema version 1 repositories; Upkeep migrates it. Schema version 3 also stores playbook references (`possiblePlaybooks`, detector `Playbooks`) as logical IDs; version 2 repositories used `.sdo/playbooks/<id>/README.md` paths. Readers must enumerate detector manifests instead of parsing one index.

Program and drift integration: controller state and broker ledgers use generated ProtoJSON. State carries ownership adoption, ordinary batching, retained incident drift and `drift_admission` retry accounting. There is no active investigator worker, task/result protocol or verdict ConfigMap. Synthetic IDs are `drift.<program>` and `drift.unexplained`. Empty repeated fields may be omitted. Removed config tags 59–61 and state tag 32 are reserved; use coordinated worker/controller images after draining. Schema version 3 classifies program Go sources, shared libraries and corpus through `memory/layout`; policy/replay tests are centralized under `memory/validate`.

Go dispatch is the reflection replay unit after the mainline cutover. Each checkpoint replays one worker result through the shared validator and real broker; broker rejection/retry history remains in the Go ledger. `PlanApplication` retains concrete applied actions, verification outcomes and timing through ProtoJSON restarts; `ProgramToolCall` retains its summary.

### Open incidents after controller restart

The controller is the only application writer. Within a process it applies a
plan, verifies independently, and conditionally rolls back on failure. A restart
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
`FailureSignals` and saves it before rollback. It survives successful or failed
rollback and restarts without retaining executable application state. A successful
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

The shared memory acceptance gate now applies replayable mitigation patches to the faulted fixture and requires a subsequent NoMatch, including after consistent renaming. Runtime and replay share SetField path semantics and JSON merge-patch behavior. `program.mitigation-not-repaired` rejects a plan that leaves the predicate matched; `program.mitigation-unreplayable` rejects effects requiring unavailable source, revision, or rollout evidence. A repaired corpus snapshot alone does not establish that the proposed mitigation produced it.

Mitigator validation is best-effort for external effects. The SDK returns a typed
`MitigationReplayResult`; a plan with any non-SetField effect passes `plan.Validate`
and receives `skipped_unreplayable_effect`, without post-plan replay. SetField-only
plans retain full faulted-fixture repair-to-NoMatch replay. Diagnosis open/fixed,
healthy-peer and rename/generalization replay remain mandatory and unchanged.
Generated replay receipts are informational; the CLI and isolated validator
report them as `MemoryCheckReport.mitigation_replay_notes`, separate from rejecting
diagnostics, using `MitigationReplayNote` status `STATUS_REPLAYED` or
`STATUS_SKIPPED_UNREPLAYABLE_EFFECT` plus program/case IDs and effect types.

Skipped replay earns no origin learning credit. Such a program needs three live,
independently verified successes and zero failures at the default trust threshold.
The controller now retains bound plan-equivalence receipts and an explicit
post-repair NoMatch for exact equivalent ad-hoc repairs; unrelated repairs in the
same outcome do not suppress that credit. Their summary explicitly says no
program mutation occurred. Every planned resource must have a successful receipt,
observed fields must equal the prepared plan, and independent outcome success
still gates trust. A Match-to-NoMatch flip alone never earns a success. Each
program earns at most one success per outcome. A failed live fast path rolls back
and blocks the revision's trust, even after the skipped mitigation passed policy.
