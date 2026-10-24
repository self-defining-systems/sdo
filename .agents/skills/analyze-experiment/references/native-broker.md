# Native acceptance and memory evidence

## Broker incident ledger

Source repairs can invalidate workload image evidence: all non-`.sdo` inputs
participate in source identity. Reconciliation publishes a current rendered lock
with per-workload gaps; unrelated objects and confirmed absences remain readable.
Count gaps separately from valid objects and incident outcomes. Memory-only
reflection preserves the lock. Rollout annotations and live images cannot restore
authority; fresh trusted deployment evidence and reconciliation are required.
A current gapped lock persists until source inputs change again.

For active Go runs, inspect the configured runtime `LedgerRoot` and the Go broker workspace root. The broker stages trusted source deployment evidence before publishing source, then reconciles that evidence to the attributed accepted commit after a crash. The evidence binds the candidate Git tree, strategy, immutable image digests, resource IDs, and observed ready revision; agent claims and annotations alone are not authoritative.

Ledger files under `.git/sdo-broker/` and the controller's runtime-state ConfigMap are canonical ProtoJSON of `BrokerLedger` and `ControllerState` (`controller_state.proto`): snake_case keys, empty and zero fields omitted (so a missing key means the zero value), RFC3339 timestamps, enums as names, and the closure as an `IncidentClosure`. The Go structs carry no JSON tags, so pre-cutover ledgers with legacy keys (for example `playbooks` or `path` in place of `playbook_ids`) are not readable by the current tools. `reflection_rejections` lists each broker rejection of a reflection proposal (`attempt`, typed `diagnostics` of `rule`/`path`/`line`/`message`/`hint`, `rejected_at`); Go re-dispatches reflection with the previous rejection's diagnostics up to twice, and `reflection_rejected` marks the terminal rejection after the bound (the closure completes with no reflection commit). Current native ledgers use `reflection_rejections`, `reflection_rejected`,
`reflection_processed`, and typed `reflection_decision` (decision, reason, logical
playbook IDs, error, SDK gaps and usage). `ReflectionDecision.playbook_ids` remains
wire field 3. A verified fast path skips reflection; exact accepted repair replay
or wholly trusted program repair may also record a skip reason. A failed learning
turn preserves the committed mitigation and records its error. The frozen reflection task's
`ReflectionRequest.depth` is `refine` (Go-decided, resumed responder session only, first attempt
only) when installed programs were applied, not dry-run or rolled back, and have bound post-apply
NoMatch tool evidence; every repair action is one of those applications; every matching binding was
applied and every confirmed root cause is covered by an applied binding; and there is no program
failure signal, drift event, exhausted loop or source repair. The prompt is then a few KB and the
expected decision is `no_change`. Empty or `full` (fresh fallback, retries, anything else) is the
complete authoring reflection. The closure's `request.state_changes` (absent in older ledgers or without a healthy baseline) is the controller's configuration diff against its last healthy baseline when the incident opened: `baseline_at`, `observed_at`, `changes` (`kind`, `name`, `change` of `added`/`removed`/`modified`, and field-level `before`/`after`, where ConfigMap data appears only as `sha256:` digests; current Secret fields are only `uid` and `resourceVersion`, with the object name on the change. Secret content drift is a resourceVersion change, never a data digest. Strict metadata-only list/watch has no full-object fallback; unsupported or forbidden metadata reads mark `Secret` unobserved. Secret type and key names are unavailable in PartialObjectMetadata and omitted. Older ledgers may contain Secret data digests), `omitted`, and `unobserved_kinds`; objects that existed unchanged at the baseline, such as SREGym's `failure-admin-*` decoys, never appear. The closure's `final_state_changes` (absent in ledgers before the verification-time state diff, and without a healthy baseline) is the same shape of diff, recomputed at verification time instead of dispatch time: a composite fault's later component can land a few seconds after dispatch while the incident is still open (K2's readiness-probe fault about 6s after its selector fault), so it can appear here without appearing in `request.state_changes`. Current derivation records these diffs for context without checking diagnosis references. Historical closures may also carry `observed_state_changes` to date cited changes against repairs; current closures omit it. `health_cleared_at` records the start of the final health-clear streak, without root-cause attribution. Program trust retains its separate bound application and NoMatch checks. The closure's `cleaned_helpers` (absent in older ledgers) lists the responder helper pods and Jobs (`sdo.dev/responder-helper=true`) the controller deleted when the responder completed. The closure's `incident_detector_states` (absent in older ledgers) holds the latest post-response evaluation of each non-health detector that raised a finding; it is learning evidence, not a closure gate. A closure with empty `final_detector_states` means health never cleared within the controller's verification window; the broker classifies it `failed` (no reflection) and strict receipt validation rejects it. A missing proposal commit is valid under `recorded-actions`; current success classification uses successful repair actions, while verified clearing without a repair is `cleared_without_action`. Receipts do not re-check diagnosis or repair evidence. Require incident IDs and commit hashes to agree with the receipt and outcome.


### Historical Python broker fields

These fields describe retired Python ledgers, not current ProtoJSON:
`reflection_attempts` counts turns including backend failures;
`reflection_backend_error` is the last such failure;
`reflection_fresh_retry_attempts` counts fresh retries after validation rejection.
`reflection_session_mode` records `fresh` or `resume`; the first turn fixes it.
The rejected diff was `<sha256(incident_id)>.reflection-rejected.diff`.
`reflection_skipped_reason` explained deterministic reuse from verified exact or
learned-detector matches and trusted successful playbooks. Such a skip used zero
reflection attempts, empty usage, a `no_change` decision with the skip reason,
and no reflection commit, accepted detector paths or rollout. Verify those
historical claims against the incident's detector, action and outcome evidence.

## Operational memory layout (schema version 3)

Detectors are registered by per-detector manifests at `.sdo/diagnostics/detectors/<health|incidents>/<id>/manifest.yaml` (the directory name is the detector ID). The former single `.sdo/diagnostics/manifest.yaml` index exists only in schema version 1 repositories; Upkeep migrates it. Schema version 3 also stores playbook references (`possiblePlaybooks`, detector `Playbooks`) as logical IDs; version 2 repositories used `.sdo/playbooks/<id>/README.md` paths. Readers must enumerate detector manifests instead of parsing one index.

Program and drift integration: controller state and broker ledgers use generated ProtoJSON. State carries ownership adoption, ordinary batching and retained incident drift. There is no active investigator worker, task/result protocol or verdict ConfigMap. Synthetic IDs are `drift.<program>` and `drift.unexplained`. Empty repeated fields may be omitted. Removed config tags 59–61 and 67–68 and state tags 32–33 are reserved; use coordinated worker/controller images after draining. Schema version 3 classifies program Go sources, shared libraries and corpus through `memory/layout`; policy/replay tests are centralized under `memory/validate`.

Go dispatch is the reflection replay unit after the mainline cutover. Each checkpoint replays one worker result through the shared validator and real broker; broker rejection/retry history remains in the Go ledger. `PlanApplication` retains concrete applied actions, verification outcomes and timing through ProtoJSON restarts; `ProgramToolCall` retains its summary.

### Controller lifecycle phases

`ControllerState` uses generated `DispatchState` and `ClosureState` enums:
`DISPATCH_STATE_PENDING`,
`CLOSURE_STATE_PENDING`, `CLOSURE_STATE_COMMITTED`, `CLOSURE_STATE_FAILED`.
Absent fields mean `*_UNSPECIFIED`; old lowercase strings and undefined numbers
are rejected. `runtime/internal/lifecycleio` persists generated messages.
`BrokerLedger.reflection_phase` drives dispatch/result inspection/review/publication/
reset/done; `reflection_results` retains completed decisions after rejection.
Frozen requests live under the ledger's `reflection-tasks/` directory.
`ControllerState.failed_closures` is an outbox, archived before the next state CAS
as content-addressed `sdo-failed-closure-*` ConfigMaps (`failed-closure.json`, label
`sdo.dev/closure-archive=failed`). Exhausted closures retain evidence and allow
later incidents; archives are runtime evidence, not operational memory.


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

### Accepted reachability baseline evidence

The optional health-owned `.sdo/diagnostics/detectors/health/service-reachability/baseline.json` records fresh failed dial samples captured by the trusted, opt-in healthy-baseline gate while all other health detectors and the authored objective independently passed. It is broker-published as `health_judge` in `initial`, followed by detector-bundle adoption. Read version, authorization/source/objective bindings, capture time, exact targets, configuration hashes and measured samples. A missing historical success alone never authorizes an exclusion. A changed source/objective/Service identity or target/backend/config/policy invalidates the record for that target; a resulting finding reports `baseline_status`. A restart reuses the accepted record, including an empty record, and restored incidents cannot capture their failures as baseline exclusions. Candidate failures in saved logs are not accepted exclusions without this measured healthy-baseline evidence.
