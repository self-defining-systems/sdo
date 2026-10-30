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
alone is the baseline. `IncidentRequest.prior_stages` and
`OutcomeRecord.prior_stages` retain typed controller-owned stage reports.
Each mitigation identifies the accepted playbook revision, bindings, plan,
`apply_receipt`, post-apply Diagnose, and cleared/remaining finding fingerprints.
Results distinguish `applied_cleared`, `applied_still_matching` (with `kept_reason`),
`reverted_ineffective` (writes restored before handoff), `rollout_failed`,
`rollout_pending_at_handoff`, `apply_error`, and `skipped` with `skip_reason`
(historical `stage_budget` meant observation exhaustion; current observation exhaustion never suppresses an admitted write).
`rollout_pending_at_handoff` whose `rollout_pending` reason says `cannot progress
without another actor` is a stall handoff, a few seconds after the write: the
replacement Pod is unschedulable, its ReplicaSet cannot create it (non-webhook
`FailedCreate`, e.g. quota or LimitRange), or the progress deadline passed. The
reason first names any old-template Pod/ReplicaSet holding its place, then the
release, then the scheduler or create message. `rollout_pending.blockers[].release`
holds the typed standard release (`RetireOldRevision` of each old revision) when a
replacement cannot be placed or quota is exceeded; SDO never applies it itself.
A stall is not a budget expiry. The rollout wait keeps a reserved share of the
deadline, so every written object has a readback; `observation_error` on a
fast object now means a real read failure, not a slow neighbour. A stalled
pending reservation releases responder writes after a 2 s grace with
`rollout_stalled` naming the blocker, not after the 90 s bound.
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
never automatically undoes a completed apply. Its recovery observation uses the shared deadline, never a serial per-candidate rollout budget. Interrupted apply intent survives in the incident tool checkpoint;
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
NotFound; generic GET errors never prove deletion. Saved scripts retain the
existing `observed-transition` proof and never claim field equivalence.
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
