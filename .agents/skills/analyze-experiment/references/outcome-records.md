# Outcome and program evidence

## Outcome records

Programs live in `.sdo/playbooks/<fault-class>/` (`diagnose.go`, optional `mitigate.go`, tests and guidance), helpers in `.sdo/lib/`, and labeled captures in `.sdo/corpus/`. Reflection edits isolated checkouts; broker acceptance requires the shared compile/generalization/rename replay validator. Presence alone proves neither use nor mitigation: join outcomes to accepted commits.

For every new captured corpus case, including additions for an existing program, `open.json` must copy controller capture bytes from `open@detect` or first-mutation `open@pre-apply` (historically also `open@settle`). See [capture boundaries](capture-boundaries.md) for phases and evidence gaps. Reflection copies selected open/fixed/primitive recordings into corpus files and labels their incident ID, selected `provenance.open_at`, digests, bindings and root-cause IDs. `healthy.json` reuses the verified fixed capture with a same-kind peer binding. Private captures live under `.sdo-runtime/sessions/<incident-hash>/corpus/` until pruning.

The shared validator compares added cases against protected captures; missing context is `program.capture-unavailable`, and incident/hash/byte mismatches are `program.capture-provenance`. Self-reported digests are insufficient. Captures omit sensitive environment values and Kubernetes write metadata. Synthetic NoMatch cases require `no_match_program_ids` and `synthetic_no_match_reason`; older cases without provenance are author reconstructions. Every accepted case, including legacy or synthetic cases, is immutable against the base commit (`program.corpus-immutable`). Add new IDs for new evidence. Changed Go must already be gofmt-formatted; the broker preserves authored bytes.

Reflection may report `learning_decision=sdk_gap` with `fault_class`, missing `primitive`, `reason` and `scratch_script`; an updated decision may also carry gaps. Join ledger `reflection_decision` to the immutable outcome by incident ID and commit. A gap is not an accepted program. Acceptance requires the reflection commit and broker validation.

An exhausted output correction records a failed worker result, ledger `learning_decision=failed`, its error, and `reflection_processed=true`; closure completes without a reflection commit. The previously committed successful outcome remains the mitigation record. Repeated identical requests are not distinct learning attempts.

`OutcomeRecord` is defined in `sdo/contracts/proto/sdodev/contracts/v1alpha1/outcome.proto`. Go stores versioned per-incident documents at `.sdo/outcomes/<sha256(incident_id)>.json`; only the broker writes them. Reflection receives typed current/prior records. Retired JSONL is removed only after complete accepted import. Join incident IDs and outcome commits to the native ledger and `production-receipt/v2`. Fields include:

- `incident_id`, `source_commit`, `deployed_commit`;
- `drift_events` retain controller-observed refs, bounded old/source/new fields, manager/operation/time and `classification` (`fault` for a learned match, otherwise `unexplained`). Current ordinary responder results may return exact event assessments with legacy `investigation_verdict`, `investigation_reason` and `investigation_evidence` spelling. Go validates them against incident observations; a bug requires source contradiction plus relation or impact evidence. Only checked benign labels in accepted outcomes suppress the identical manager and current/source observation after restart/reset. Bug and unsure never suppress repeats. A benign no-op is `cleared_without_action`, with no mitigation/trust/learning credit. State retains bounded drift history plus exact checked source-inventory acceptances in `accepted_source_drift`, bound to application-source identity in `accepted_source_drift_revision`; these acceptances do not grant cause verification or repair credit. Health and every retained synthetic drift finding must clear; source errors, missing bindings and Unknown remain unresolved. Learned deletion needs explicit NoMatch per retained binding. Ordinary captures use the incident session's `open@detect` phase. Ownership adoption retains counts, managers and retry diagnostics. Historical runs may contain investigator state and a separate verdict ConfigMap; current releases remove both.
- detector history, findings, and final health-detector state;
- surfaced, inspected, confirmed, rejected, and applied playbooks;
- confirmed root causes. Newer results give each cause `evidence` (live items only: `detector-finding`, `synthetic-traffic`, `state-change`, or `live-observation`, each with `source` and `observation`), `explained_detectors` (detectors that must clear after the fix), and `static_context` (scripts, manifests, ConfigMap bodies, or notes that motivated the hypothesis but prove nothing). Older records have these empty;
- `classification`: current records use `failed` (responder failure, dispatch error or health or admitted-source-drift verification timeout), `cleared_without_action` (verified health with no accepted source repair and no successful repair action), or `success` (verified health and a repair). Successful outcomes remain eligible for prior-outcome recall. Failed outcomes can dispatch reflection only for independently verified repaired causes, with filtered learning evidence (see [partial repair reflection](native-broker.md#partial-repair-reflection)); their classification and full-mitigation/trust credit remain failed. Root-cause claims are no longer independently verified: current outcomes omit `diagnosis_verification`, and closures/state omit `observed_state_changes`/`incident_observed_changes`. Historical outcomes may contain those fields and the retired `partial`, `external_recovery`, `false_positive`, `false_negative` and `cancelled` classes; interpret them under their original policy rather than treating them as current success;
- repair and memory commits, plus structured live repair-action receipts; newer receipts carry `resources`, the objects each action mutated (older ones only a free-form `target`, parsed as `[namespace/]Kind/name` when possible);
- responder backend/model and transport-reported usage (`llm_calls`, `input_tokens`, `cache_read_input_tokens`, `cache_write_input_tokens`, `cache_write_1h_input_tokens`, `uncached_input_tokens`, `output_tokens`, `reasoning_output_tokens`, `model_requests`, and `total_cost_usd` when the provider reports it). `cached_input_tokens` is the deprecated alias of cache reads; a record without `cache_read_input_tokens` predates agentshim 0.7, and on Claude its `cached_input_tokens` also counted cache writes (subtract `cache_write_input_tokens` to get reads);
- detected, dispatched, mitigated, verified, and completed timestamps.
- `program_tool_calls` (command, time, request and response JSON, error), `program_runs` (program ID, finding fingerprint, binding source, bindings, verdict, duration, program revision, optional `can_mitigate`), and `plan_applications` (application ID, program IDs, plan, applied/dry-run flags, verification, error, duration, rollback state, applied actions, timing and optional final-equivalence proof; rollback pre-images and patches are not durable outcome fields). `binding_source` is `finding`, `hint`, or `sweep`; sweep runs have no finding fingerprint. Older runs omit this field. `response_loops_used` counts apply batches; `response_loop_exhausted` marks an unresolved budget stop. `program_failure_signals` distinguishes a matching diagnosis after apply, residual detector findings, newly surfaced findings, and ad-hoc coverage gaps. Read the [program trust rules](fastloop-results.md#program-trust-evidence) before counting successes. Every recurrence dispatches a responder; coverage and counters inform its choices. See [agent-selected repair evidence](automatic-repair.md) for selection and attribution.

Tool responses expose logical playbook IDs. The outcome keeps a structured summary with `truncated: true` when a full tool response would exceed 2 KiB. A clean `verify` omits the unrelated namespace sweep. For verified repairs, `timestamps.mitigated_at` is the latest successful repair completion no later than closure completion, falling back to responder completion when no valid completed repair receipt exists.

When a responder completes but health does not clear or admitted owned-field drift on a source-declared object in the incident namespace remains unresolved within the controller's verification window, the controller closes the incident with empty `final_detector_states`; the outcome is classified `failed`. Source-comparison errors also keep verification waiting. The outcome retains the admitted `findings` and `drift_events`; the closure's `incident_detector_states` records the latest firing or error evaluation. An image repair and clear readiness alone cannot establish success while admitted targetPort drift remains. Restoring the admitted fields to accepted source permits closure; unowned or undeclared changes are not new blockers. Late drift can attach after dispatch to the same incident, so inspect the final closure/outcome rather than only the frozen worker request. Historical dev closures before FX2 checked health alone; do not retrospectively assume they ruled out admitted drift. No outcome schema or classification names changed.

Useful queries for the active per-incident outcome documents:

```bash
jq -s 'map({incident_id, classification, repair_commit, memory_commit})' .sdo/outcomes/*.json
jq -s 'group_by(.classification) | map({classification: .[0].classification, count: length})' .sdo/outcomes/*.json
```

Program-run `can_mitigate` records whether the compiled registry has a mitigator; `false` means diagnosis-only, and an absent field on older evidence means unknown. It does not establish trust, plan validity or replay support. Responder prompts omit NoMatch runs and report `omitted_no_match_program_runs`; durable requests, outcomes and captures retain the full runs.

Go may add deterministic `repeat-<digest>` corpus cases through normal reflection validation and broker publication without dispatching an LLM, when every cause/matched binding has verified plan-equivalent repair evidence. Corpus-only decisions are `updated` with no changed playbook IDs; no README annotation is needed. Trust consumes only the covered instances in the controller-owned, pre-commit eligibility result; adding corpus files supplies no independent credit. Runtime dependency revisions now exclude `_test.go` as well as Markdown; older runs can reset trust on test-only changes. A cleared bound program with another explicit open resource finding is not a failed application; global health is independently required for the final outcome. Older traces can contain false `diagnosis_clear_detectors_firing` and application-failure signals from composite incidents, which prevent trust even when each bound apply response is NoMatch.

OutcomeRecord.reflection_eligibility version 1 durably stores Go-owned typed items before the outcome commit. ReflectionRequest.cause_work carries those items grouped by the exact confirmed_root_causes indices/resources; unassigned_work contains ungrouped observations. Covered items require verified equivalent repair; fixed_capture_match items preserve exact fixed snapshots as required NoMatch corpus cases. No other question grants credit. PlanApplication.final_equivalence_verified must be affirmatively true for covered eligibility; false or absent means unproven. An absent fixed verdict is also a question. Fixed negative corpus cases have only no_match_program_ids and provenance naming the SDK fixed-negative phase; they cannot earn origin credit. Validation retries retain the responder session and typed work/diagnostics.

Current reflection workers can materialize positive cases through the typed `create_corpus` tool. It copies the controller open/fixed/recording bytes and derives phase values and SHA-256 provenance; it does not grant acceptance. The broker still compares the proposed bytes independently. The typed `check_memory` input is unwrapped ReflectionResult fields; the final model reply retains the session envelope with `reflection` populated and `incident_result` null. The generated worker guide is `sdo/skills/sdo-memory/references/runtime.md`; generated program API documentation is `sdo/skills/sdo-playbook-authoring/references/program-sdk.md`.

Reflection evidence browsing is a bounded projection only: inventory and field pages carry the accepted outcome commit, incident ID and capture digests. Oversized page items require narrower field selection, not a conclusion that evidence is absent. Full captures and the committed outcome remain authoritative.

`reflection_evidence_gap_count` on the native ledger and first-party incident row counts distinct confirmed
resource/phase pairs with no observation in the complete protected recordings or controller object snapshots.
A captured symptom object counts as observed even when no program primitive read it. Historical absence is unknown,
not zero. Recorded errors count as present calls, not as successful evidence. `decline_reason=evidence_gap`
identifies an actual no-change decline; Go rejects it when no gap was flagged. A nonzero presence-gap count
alone does not establish blocked learning, and already-proven covered repairs still skip the model.

Evidence-gap items and the distinct resource/phase count are part of the same pre-commit `reflection_eligibility` result. The native ledger projects that saved count for receipts; it does not reclassify recordings during reflection retries. Historical outcomes without an indexed count remain unknown, rather than zero.


Structured plan effects: `SetField.value_from_source` selects the incident-bound accepted manifest; `document_format` and `document_path` identify a JSON/YAML entry within a ConfigMap key. Intent is not health proof. Current execution uses incident namespace scope, current field/UID checks, informer barriers, independent verification and rollback. Missing or stalled observations remain unknown. Preimages stay process-local; durable protobuf applications never acquire mutation journals or replay authority. Historical XV1 traces additionally used expiring content authorization; see [automatic repair evidence](automatic-repair.md).

Object lifecycle receipts add `PlanApplication.object_effects`: operation, ref, server UID, state (`not_applied`/`applied`/`unknown`), confirmed direct observation, and irreversibility. A delete includes a `deleted_pre_image` for inspection only; it is never a restart mutation journal or recreation authority. Create ownership requires its server UID receipt; an identical object after a lost create reply is still unknown. Successful API deletion without confirmed named absence is not verified repair. These effects report `skipped_unreplayable_effect` in corpus replay and earn no origin credit.

Plan application JSON patch `Authorization` describes value provenance in GS1. See [automatic repair evidence](automatic-repair.md) for current labels and historical policy-grant meanings. It does not establish verification or trust credit.


Plan applications now expose `action_reports`: each report names its action index, status (`prepared`,
`applied`, `refused`, or `unknown`), and value provenance. Source values retain `source_commit`, reference and
field path; observed values retain the invocation snapshot's `resource_version`, reference and path; scalar
parameters retain their declaration name. Reports contain no resolved value. Exec reports only `exit_code`,
never stdout/stderr; a refusal or unknown outcome is not verification evidence. DeletePod records an
irreversible named UID effect without a Pod body or recreation authority. ScaleWorkload verifies replicas
through a direct read. Captured snapshots preserve resourceVersions for observed-value provenance while
stripping UID and write metadata. Program bindings now separate `objects` and scalar `values`; object-only
historical corpus bindings remain readable. These receipts describe evidence, never restart mutation journals.
Controller restart state externalizes verbose incident request/result/closure evidence above 32 KiB to immutable canonical ProtoJSON under `<repository-mount-path>/.sdo-runtime/controller-evidence/<sha256>.json`. The ConfigMap annotation `sdo.dev/state-evidence-sha256` binds those bytes to the state revision. The restart document omits initial program runs, result program runs/tool calls and frozen task bytes, including their pending-closure copies. Read the archive for full evidence when inspecting state offline; the storage edge hydrates it before closure/outcome/receipt derivation. Missing or corrupt archives fail closed. Archives are written and synced before the ConfigMap CAS and retained across ambiguous writes and proposal cleanup. Outcomes and strict receipts retain their existing complete evidence contracts; this is a restart-storage split, not a truncation of program history.

Cold partial reflection uses the latest complete controller verification after a
trusted timed recorded mutation; no installed program is required. Over-cap verify
records retain a bounded typed `verification_summary` with complete open-resource
coverage and detector evaluation times. Missing/overflowed summaries cannot prove
absence. Resource-related findings and later mutations block clearance. The full
failed outcome remains authoritative; reflection receives only independently
repaired cause groups and scoped verification evidence. This is learning eligibility,
not global health or program trust; authored program bytes still pass the existing
capture provenance and corpus replay validator. No fixed snapshot is fabricated.

A complete healthy controller verify with zero findings also proves individual
repairs when the global closure is failed. It requires all evaluated detectors
clear and no unresolved flag; classification stays failed. No peer fault is required.

RL1 typed SDK gaps include resources to scope a decline to one confirmed cause; another verified cause still requires its program and detector. First-encounter replay learning can earn one trust credit even when the program did not exist during the repair. This requires a broker-validated reflection commit associated with the controller-verified origin; a later verified program failure is not erased by replay. Controller corpus provenance is optional for session-evidence fixtures; no benchmark grade or recovery verdict is a verification source.

### Applied writes with a pending rollout

`plan_applications[].rollout_pending` is controller-owned evidence that writes landed but a rollout did not complete within its per-mitigation budget (90 seconds, capped by the original response deadline). Its blockers contain resource identities, observed conditions and owned warning events, including Pod creation/admission errors. It is not a verified repair: `verification_completed` and `verification_passed` remain false and the verdict is Unknown. Pending writes stay applied; rollback remains an explicit responder decision. A pending rollout supplies no success/failure credit by itself. Later independent verification remains authoritative. A truncated tool record may retain only a bounded blocker excerpt; the typed native application keeps full evidence.

Incident-open `playbook_coverage` is copied from the controller request into outcome field 35. The match and unexplained-finding lists retain their initial deterministic ordering. Match IDs and bindings link to application/action receipts; coverage alone is not repair attribution. See [agent-selected repair evidence](automatic-repair.md).

Responder prompt accounting now groups the short workflow routing under
`responder.instructions` and the compact request, accepted intent and latest
detector evaluations under `responder.incident_data`. Warm/cold strategy,
inlined playbook bodies, separate verification/no-action paragraphs and their
former part names were removed; the responder loads `sdo-incident-response`
instead. Optional transport instructions remain `responder.additional_context`.
The request still carries controller-built playbook coverage and actionable
initial program evidence, with NoMatch sweeps counted as omitted. Large facts
use a private temporary JSON file for the response turn; the bounded inline
pointer is not evidence that findings or intent were discarded. Full worker
requests and durable outcomes remain authoritative. Reflection prompt context
and responder-session resume behavior are unchanged by this responder rewrite.

Failed closures retain fresh post-response detector observations in typed closure `partial_detector_states` (field 21), projected into outcome `detector_history`, including health-detector peers. `final_health_detector_state` remains empty when global closure is unverified; partial clear evidence never establishes recovery. Clearing persistence must finish before a finding is absent from the closure observation. The broker binds trusted mutation receipts and opening primary-resource findings to the latest confirmed detector observation, retains only independently repaired causes/actions in the reflection learning view, and leaves excluded causes as evidence-gap questions. A reflection `no_change_reason` describes that filtered view, not missing evidence in the complete durable outcome. Pending, error, stale or still-present findings confer no executable-learning authority.

Early closure groups synthesized opening findings by repaired primary resource. When a final report arrives during report grace, its cause owns the resource and retains the independent opening evidence/detector links from the synthesized cause. Missing-report, returned-report and late-closure outcomes use the same independent repair validator; synthesized duplicates must not remove reflection eligibility. Overlapping model-authored claims remain subject to the ordinary ambiguous-ownership rejection.

Working program tools retain `program_revision=working-tree:<digest>` in program runs, menu options and recorded tool responses. The digest uses the same source/import-closure calculation as accepted program revisions, excluding Markdown and test-only files. The prefix explicitly distinguishes unpublished checkout code. `program_revisions` in plan/apply/mitigate responses binds each program to that executed revision; prepared plans keep their own revision through later edits. Outcome enrichment preserves working revisions rather than replacing them with accepted-source hashes. Treat these as separate revisions for trust; successful execution does not publish memory. The broker projects source-only repairs and carries working playbook/shared-library diffs into reflection for the usual memory check and independent validation.


## Shell code path

An explicit `code_path` object records the selected programming model and optional
seam overrides; absent means the default Go path. `name: shell` uses ordinary
versioned `.sdo/outcomes/<sha256(incident_id)>.json` outcome documents and the same
controller closure evidence. A fired-script finding carries its path and bounded
stdout/stderr evidence, plus accepted-commit/activation metadata. Empty playbook
coverage is intentional; shell learning has no Go compile/replay or program schema.

Accepted scripts live under `.sdo/diagnostics/detectors/{health,incidents}/<id>/`;
notes and scripts live under `.sdo/playbooks/<id>/`. Ownership remains judge health,
responder incident/playbooks, and controller outcomes. Check reflection paths and
accepted commits to identify learned files. Do not infer Go-program credit from
shell notes. Script timeouts/crashes are detector errors, never repaired faults.
