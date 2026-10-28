# Ordered response stages

`ControllerConfig.code_path.overrides.response_stages` defaults to `deterministic,agent`; `agent`
alone is the baseline. `IncidentRequest.prior_stages` and
`OutcomeRecord.prior_stages` retain typed controller-owned stage reports.
Each mitigation identifies the accepted playbook revision, bindings, plan,
`apply_receipt`, post-apply Diagnose, and cleared/remaining finding fingerprints.
Results distinguish `applied_cleared`, `applied_still_matching` (with `kept_reason`),
`reverted_ineffective` (writes restored before handoff), `rollout_failed`,
`rollout_pending_at_handoff`, `apply_error`, and `skipped` with `skip_reason`
(`stage_budget` means the 45 s stage budget ran out before the match was applied).
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
The stage applies matched bindings sequentially, re-running Diagnose on fresh
state after the preceding application returns. A pending handoff
(`admission_blocked`, stalled or bounded rollout) keeps the pass: later matches
still get their fresh diagnosis and may unblock it. Only a failed application
(`apply_error`, `rollout_failed`) ends the pass; matches after it have no stage
entry. Runs before this rule ended the pass at the first pending handoff. A nonmatching or
unavailable diagnosis gets one deferred turn after the other candidates, within
the same 45 s budget. Each applied mitigation has its own `apply_receipt` and
`application_id`; `plan_action_start` is zero and `plan_action_count` covers its
plan. Historical merged-stage runs share one application ID and use the action
range to select each mitigation's actions. Recorded stage `apply`
tool calls carry `"stage":true` in `request_json`; agent applies do not.
Stage-level fingerprints describe work remaining at each handoff. These scoped
observations never replace independent controller closure/classification.

The deterministic stage requires a fresh Match, exactly one mitigation entry
point, and all parameters bound from covered findings. It has no trust gate and
never automatically undoes a completed apply. Its own rollout wait is capped at
30 seconds. Interrupted apply intent survives in the incident tool checkpoint;
a retry reports uncertainty instead of repeating a potentially landed mutation.

`resolution_path: deterministic` means the incident needed no responder worker;
usage is zero model work, while recorded plan actions remain repair evidence.
These incidents still receive reflection using a fresh session and the same
stage reports; a missing session from a failed responder does not enable that
fallback. Attribute learning only from the accepted reflection and independently
verified cause evidence. The responder's inline view summarizes stages; complete
receipts remain in its private incident facts and committed outcome.

### Receipts after responder retirement

The controller may retire a responder Job after an early report. Post-reflection receipt collection uses the broker ledger's frozen incident request, correlated result, durable dispatch/completion timestamps, independently verified closure, and accepted acknowledgment. A live responder Job is not a receipt prerequisite. An empty `responder_job_evidence` means no Kubernetes success claim is made; the Go checker requires valid ordered lifecycle timestamps and a completed correlated result instead. Historical receipts with Job evidence retain their original validation.
