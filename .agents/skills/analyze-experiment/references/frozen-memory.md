# Frozen-memory repeats

`--frozen-memory` suppresses all closure publication, including source proposals,
outcome documents, captured learning artifacts and reflection. Independently derived
outcomes remain in the private native broker ledger, with `frozenMemory: true` and
`reflectionSkippedReason: frozen_memory`. They never enter accepted memory, program
trust or subsequent responder snapshots. The receipt's `outcomeCommit` identifies
the unchanged accepted base; it does **not** identify a published repeat outcome.
The durable policy cannot change on retry/restart. Force reflection conflicts with
frozen memory. Repair actions against the running cluster are still available.

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
