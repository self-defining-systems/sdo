# Ordered response stages

## Incident workspace preparation

Incident workspace preparation uses the full accepted commit bound in the
durable opening request. It clones that immutable revision under a per-proposal
lock, independently of the shared publication lock held during peer validation.
Newer memory is incorporated at later publication/rebuild boundaries. Compare
`incident_opened` with `workspace_prepared` in lifecycle telemetry; current logs
also emit `incident_workspace_preparing` and `incident_workspace_prepared` with
`bound_commit`, the resulting `base_commit`, `duration_ms`, and any error. A
workspace's base must remain the opening revision even when a peer commits in
between. Older builds selected the latest head only after waiting for the shared
broker lock, so a long pre-stage gap could be peer validation time rather than
program execution or a controller restart.

## Stage reports

`ControllerConfig.code_path.overrides.response_stages` defaults to `deterministic,agent`; `agent`
alone is the baseline. A page that opened the incident (`page` set, no `opening_keys`) starts the
responder directly, even during startup. Read `responder_dispatched` for the
dispatch and `escalated_at` for a verification-bound closure without proof.
A page-only alert set uses the responder's explicit verification of the reported
operation and controller settlement evidence. Joined alerts use detector proof.
`IncidentRequest.prior_stages` and
`OutcomeRecord.prior_stages` retain typed controller-owned stage reports.
Each mitigation identifies the accepted playbook revision, bindings, plan,
`apply_receipt`, post-apply Diagnose, and cleared/remaining finding fingerprints.
Results distinguish `applied_cleared`, `applied_still_matching`, `rollout_failed`,
`rollout_pending_at_handoff`, `apply_error`, `escalated` (a matched Diagnose whose verdict is
`reach: out-of-reach`, from a Diagnose-only playbook or a plan whose only action is `Escalate`: nothing
was written, `escalation` holds the verdict, its targeted findings leave the stage's work and the
synthesized cause carries `cause_class`/`reach`, so closure derives `escalated` while they still fire),
and `skipped` with `skip_reason` (a Diagnose-only playbook with any other verdict is still `no_mitigation`)
(historical `stage_budget` meant observation exhaustion; current observation exhaustion never suppresses an admitted write).
`holds_data` (historic only; current stages never skip for it) meant the plan would have restarted, deleted, scaled down or rewritten a workload the accepted application map marks `holds_data`, or one of its Pods; `error` named the action and nothing was written. A learned restart of a `holds_data` workload now runs like any other match.
Data effects never skip an attempt. External actions (`Exec`, `EnsureExec`, `EnsureHTTP`, `RunScript`, `RunJob`, `EnsureDatabaseUser`) and typed store actions (`StoreBackup` `read`, `InvalidateDerived` `derived-invalidate`, or `data-write` on a target the map does not mark derived, `RepairData`/`RestoreBackup` `data-write`, `RestoreStoreItem` `store-setting`, `CheckStoreCredential` `read`) run on the automatic path; their effect on each `holds_data` component is recorded as a `data effect:` line in the closure's `source_check_notes` (see [outcome records](outcome-records.md)). Runs before the record-only change could skip them with `holds_data` (error naming the effect class or the `derived_from` source).
`already_attempted` now means every write of the match (same action, object and resolved values; `Reason` ignored)
repeats a write an earlier attempt of the same stage applied whose effect is unsettled (`rollout_pending_at_handoff`
or `applied_still_matching`) and that is still in progress when the match is decided (read live: a
touched Deployment still rolling out, or the 90 s recovery window after the write still open; the same holds after a
controller restart, where the write is found in the restored applications); the earlier attempt keeps settlement credit. Before this
rule, a second program restarting the same Deployment superseded the first (`SETTLEMENT_REJECTION_REASON_SUPERSEDED`).
A plan application rejected with `SETTLEMENT_REJECTION_REASON_EFFECT_NOT_CONFIRMED` carries `effect_confirmation`: `reason`
(a failure before any comparison, such as no pre-image or an unreadable or replaced object), `mismatches` (object, path, expected,
observed; Secret data redacted) and `rollout_status` when settlement ran. Its `error` starts `effect_not_confirmed:` and replaces
the apply-time error (often a stale `rollout_pending`).
`rollout_pending_at_handoff` is the apply receipt's pending-rollout label. The controller owns the actual handoff decision. Join `apply_completed_at`, `recovery_deadline`, the optional `stability_deadline`, and `handoff_keys` to episode timing. `rollout_unsettled_at_deadline` is terminal trust evidence for the recorded program revision. Historically a stalled rollout could trigger an early handoff or a separate lag wait.

The stage applies matched bindings sequentially with a single rollout sample, re-running Diagnose on fresh
state after the preceding application returns. It then observes all pending applications under one durable shared recovery deadline (up to 90 seconds, bounded by the response deadline). A pending handoff
(`admission_blocked`, stalled or bounded rollout) keeps the pass: later matches
still get their fresh diagnosis and may unblock it. A failed application does not starve independent matches. After writes, uncovered faults, execution failures and verified admission dependencies hand off to another actor. Runs before this rule ended the pass at the first pending handoff. A nonmatching or
unavailable diagnosis gets one deferred turn after the other candidates, without increasing write authority. A pending stage snapshot is superseded for reflection staleness only by a later verified final application with the same ID, plan, program IDs and start time. Missing, failed, rolled-back or ambiguous final receipts cannot excuse it; unresolved own findings and later hand repairs still require refinement.
Each applied mitigation has its own `apply_receipt` and
`application_id`; `plan_action_start` is zero and `plan_action_count` covers its
plan. Historical merged-stage runs share one application ID and use the action
range to select each mitigation's actions. Recorded stage `apply`
tool calls carry `"stage":true` in `request_json`; agent applies do not.
Stage-level fingerprints describe work remaining at each handoff. These scoped
observations never replace independent controller closure/classification.

The deterministic stage requires a fresh Match, exactly one mitigation entry
point, and all parameters bound from covered findings. It has no trust gate and
never automatically undoes a completed apply. Controller closure observes recovery against the fixed deadline after the stage publishes its receipts. Exit reason `UNCOVERED` with a handoff before that deadline means an alert the settled writes cannot clear (a condition, user test or request-error alert none of whose map components depends on a written one), or a user test still failing over its own full window after they settled, started the agent. A `DEADLINE` away from `recovery_deadline` means the rollout settled later (the wait counts from the settle, at most 10 minutes; the estimate itself is at most `response.after_playbook_wait`) or Kubernetes gave up on it (earlier). Interrupted apply intent survives in the incident tool checkpoint;
a retry reports uncertainty instead of repeating a potentially landed mutation.

`resolution_path: deterministic` means the incident needed no responder worker;
usage is zero model work, while recorded plan actions remain repair evidence.
Fully covered repairs first use the broker shared no-change check without a learning worker, even when a watching responder session exists. Otherwise these incidents receive reflection using a fresh session and the same
stage reports; a missing session from a failed responder does not enable that
fallback. Attribute learning only from the accepted reflection and independently
verified cause evidence. The responder's inline view summarizes stages; complete
receipts remain in its private incident facts and committed outcome.

### Receipts after responder retirement

The controller may retire a responder Job after an early report. Post-reflection receipt collection uses the broker ledger's frozen incident request, correlated result, durable dispatch/completion timestamps, independently verified closure, and accepted acknowledgment. A live responder Job is not a receipt prerequisite. An empty `responder_job_evidence` means no Kubernetes success claim is made; the Go checker requires valid ordered lifecycle timestamps and a completed correlated result instead. Historical receipts with Job evidence retain their original validation.

At independently verified recovery, the controller settles accepted stage
applications once using fresh rollout/readback, the same bound program revision
and diagnosis, and final object-field equivalence. A converged application whose
diagnosis is NoMatch becomes `applied_cleared`; its final `apply_receipt`, bound
`post_apply_diagnose`, and outcome application supersede the earlier bounded
handoff observation. No writes or restored rollback authority are involved.
Execution failures, changed object UIDs, superseded writes, unknown observations,
and genuine source residuals remain unverified and available to reflection.
Readback/drift comparison excludes new undeclared scalar fields only when equal
to the API's isolated dry-run rendering of accepted source; changed accepted
values and API errors remain evidence. No receipt fields or schema change.
Covered-repeat admission and the broker's no-new-evidence check consume this
settled evidence; successful global health alone never grants a no-change skip.
The generic-health-first detector-learning gate remains unchanged.

Covered `pod-admission-rejected` Namespace findings take precedence over workload
repairs, independent of program names or coverage order. Pending application
checkpoints retain `Refs` for observation and `WriteRefs` for exclusive writes.
An absent `WriteRefs` uses the legacy conservative scope; an empty list means no
exclusive writes. Applied no-ops and `RetryAdmission` requeues hold no exclusive
reservation, so a prerequisite repair can retrigger blocked ReplicaSets. This
does not establish rollout success or grant rollback authority. Actual desired
state writes and uncertain writes retain conflict protection.

Terminal responder dispatch errors preserve the worker Job failure as their
cause, including curated conditions and container termination evidence. Warm
startup preceding injection excludes Pod-create rejection as the cause of that
worker's later exit; missing termination evidence cannot identify its cause.

Recovery settlement uses `plan_applications[].final_verification_basis=recovery-settled`,
plus the same application ID, accepted dependency revision and exact bindings in
`prior_stages[].mitigations[]`. This receipt requires confirmed applied actions,
scoped health clearing, bound final NoMatch, final field equivalence and no
superseding corrective write to the causal resources. Health may clear inside
the write interval; receipt completion is not the effect time. Timeout, pending,
unconfirmed and truncated observations are neutral, never execution failures.
Actual partial/execution failures, rollback and exact false matches remain adverse.
Independent repairs on other resources do not supersede the application.
Restart checkpoints retain observation preimages and patches without restoring
prepared plans or rollback authority. Final receipts and covered work drive
classification, learning admission and trust. Historical outcomes are immutable;
missing historical proof grants neither inferred success nor inferred failure.

The same recovery owner observes responder-applied saved programs. Private
`ApplicationBindings` checkpoints retain accepted revisions, exact bindings,
finding scope and action ranges before mutation; they restore no plan handles.
At recovery their observations appear in the existing `agent` stage report.
Shared batch receipts require every bound scope to settle. Confirmed unchanged
objects and admission requeues retain ADMIT1's observation-only scope when
checking desired fields. Confirmed UID-bound deletes require a fresh direct
NotFound; generic GET errors never prove deletion. Every action kind whose
effect lies outside Kubernetes object fields (`planwire.NonObjectEffect`: Exec,
EnsureExec, EnsureHTTP, EnsureDatabaseUser, RunJob, RunScript) settles on the
`observed-transition` proof instead of field equivalence: each such action
recorded its own success (exit 0; an ensure observed off Expect before and at
Expect after; catalog read-back; Job status; script receipt), then the bound
Diagnose that matched before the plan is NoMatch with scoped health clear. They
never claim field equivalence. Before PX10 only scripts had this proof, so an
EnsureExec repair always settled `EFFECT_NOT_CONFIRMED` / `rollout_failed`.
Transient post-apply Match/new-finding observations remain typed diagnoses and
findings rather than additional failure strings. Historical stored failure
signals and accepted outcomes remain immutable. `rollout_stalled` is the durable
reason code for a measured dependency requiring another actor, distinct from an
observation timeout.

For closures with a responder session, settlement reads the existing recorded-API
write projection before outcome derivation. A later overlapping raw API write
prevents attribution; incomplete recorded history is neutral. Known automatic
writes within their own original interval are not a later responder correction.
A returned result omitting applications cannot erase complete controller-owned
checkpoint facts. Missing checkpoint facts still grant no positive credit.
The storage edge distinguishes positive API receipts from complete recovery
history: an unpaired, partial or unresolved relevant mutation prevents credit.
A recorded corrective child write also follows its controller-owner lineage;
a Pod receipt ending at a ReplicaSet cannot evade a Deployment's attribution
check. These are neutral attribution gaps, not program execution failures.

Read `prior_stages[].recovery_wait` as the controller's disposition of the automatic attempt. Its `duration` is elapsed time from apply completion to the controller's decision. `CLEARED` means Recovered held; `DEADLINE` means the bound was reached without proof; `NOTHING_APPLIED` means no live write was observed. Historical reports may repeat the field on application receipts and use the old `BLOCKER`, `FAILED` and `UNCOVERED` exits. Interpret them against the build that produced them. The optional stability deadline records the one extension when fresh clearance and settlement are confirmed. A missing sample keeps closure pending. Handoff requires a freshly firing alert-set member under the agent policy.

`plan_applications[].settlement_rejection_reason` records the typed closure
attribution veto (also copied into stage apply receipts). Unspecified means no
veto; it is not success by itself. Reasons distinguish missing dependencies or
snapshots, incomplete execution, rollback, supersession, unknown history,
revision/binding gaps, pending/failed rollout, source residuals, uncleared
diagnosis/health and unconfirmed effects. Earlier positive scoped verification
and field observations remain historical facts when a closure gate rejects;
the rejection still withholds repair credit and trust. Plan execution completion
uses the producer's action reports, including successful waits and satisfied
ensures, rather than counting mutations.

Live API history uses a producer-drained manifest prefix bounded through closure
(`snapshot_through`), including asynchronous body projections. Writer liveness
alone does not invalidate that snapshot. Real drops, disabled recording,
changed generations, unresolved mutations and missing checkpoints remain unknown
and grant no attribution. Confirmed workload removals retain their causal write
scope but settle through UID-bound delete evidence and fresh direct absence,
without asking the absent workload to roll out.

Cross-resource Service admission additionally requires an availability/traffic
or declared-window role; a Service configuration/selector fault is not a rollout
symptom just because it selects or names a nearby workload. The saved handoff
is the task publication authority. Worker claims use that decision without a
new health read. Published requests, executions and workers retain ownership
and are rejoined.

## Fixed by the playbook alone

A repeat counts as fixed by the playbook alone only when all hold, in every arm, with seconds taken after inject:
no responder was dispatched and an automatic-stage plan application ran; the users had harm (their last bad second
came after the fault's onset); and recovery followed the playbook's write (the write started before the last bad
second, and no bad second came more than a short settle allowance, 15 s, after the write completed). A harm that
ended before the write, or a fault that never hurt users, is not credited to the playbook. The record's `users`
series is indexed from `users.start`, not from inject: shift it by `users.start - inject_at`.

A terminal validation refusal is an `apply_error` with `skip_reason: plan_not_executable`. The attempt records the playbook, revision and rejecting check without a write receipt for that plan. It ends automatic execution and counts one failure for that revision in the incident. Historical skipped attempts with that reason do not count as terminal failures.
