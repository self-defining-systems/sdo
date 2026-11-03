# Frozen-memory repeats

`--frozen-memory` suppresses all closure publication, including source proposals,
outcome documents, captured learning artifacts and reflection. Independently derived
outcomes remain in the private native broker ledger, with `frozenMemory: true` and
`reflectionSkippedReason: frozen_memory`. They never enter accepted memory, program
trust or subsequent responder snapshots. The receipt's `outcomeCommit` identifies
the unchanged accepted base; it does **not** identify a published repeat outcome.
The durable policy cannot change on retry/restart. Force reflection conflicts with
frozen memory. Repair actions against the running cluster are still available.
