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
