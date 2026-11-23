# Frozen-memory repeats

Operator config `mode: frozen` (`SDO_MODE=frozen`; the pre-config flag was `--frozen-memory`) suppresses all closure publication, including source proposals,
outcome documents, captured learning artifacts and reflection. Independently derived
outcomes remain in the private native broker ledger, with `frozenMemory: true` and
`reflectionSkippedReason: frozen_memory`. They never enter accepted memory, program
trust or subsequent responder snapshots. The receipt's `outcomeCommit` identifies
the unchanged accepted base; it does **not** identify a published repeat outcome.
The durable policy cannot change on retry/restart. Force reflection conflicts with
frozen memory. Repair actions against the running cluster are still available.

`learning.closing_turn: false` is the narrower switch for runs whose learning is discarded (the cold and p2 arms of
a learning plan; only p1's memory seeds another arm). The closure is an ordinary operate-mode closure: the outcome
document is committed to accepted memory exactly as in a learning closure (same document, trust counters and source
publication), but the ledger has `requires_reflection: false`, no `reflection_decision` and
`reflection_skipped_reason: closing_turn_off`. No closing turn, cleanup turn, corpus capture, reflection commit or
rollout follows the acknowledgment, so `closure_phase_timestamps` stop at `closure_ack` (plus source publication
phases when a committed source repair was owed). Do not read the missing closing turn as a failure, and compare
`learned`/closing-turn cost only across arms whose setting matches. Detection, dispatch, the responder or automatic
stage, verification and grading are unchanged. The lanes workload keeps the setting out of the lane's install
identity, so arms that differ only in it still reuse one warm lane.

Fastloop ablation rows carry `ablation_arm` (`full` or `detectors-only`).
`ablation-memory.json` beside the rows records repository, branch and the fetched
immutable commit used as the row-start planned revision. These arms force replay
off and frozen memory, and install supplied memory without lifecycle/judge work.
Startup refresh is suppressed; health inputs must still match the compiled inputs.

Frozen receipts name an existing base, so they do not require a new memory-host
activation. Activation continuity remains required for published-memory commits.

Playbooks-only uses `IncidentRequest.external_message`, with no findings/history,
health catalog or automatic stage. `ControllerObservation.completed_external_incident_id`
is completion, not verified health (also preserved after ACK). The frozen broker
outcome remains `unconfirmed`; strict receipts accept this only with explicit external
completion, a completed responder, no detector authority and unchanged accepted base.
Use the oracle result for success. `incident_resolution_scope` is
`external_trigger_to_responder_completion`; `verified_at` in the common benchmark
resolution projection is the responder-completion row boundary, not a health claim.
Trigger/detection time is controller admission after injection; first successful
repair action and responder completion populate the ordinary timing fields.

For playbooks-only, the common injection-to-detection, mitigation and resolution
metrics start at injection end; raw injection-to-mitigation remains anchored at
injection start. `first_repair_action_at` is the earliest successful repair's
start; `injection_to_first_repair_action_seconds` and
`injection_to_responder_done_seconds` use the same arm-specific origin.
