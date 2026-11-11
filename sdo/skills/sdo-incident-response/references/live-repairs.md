# Live repairs

For detector-driven incidents, SDO's independent verification is the source of truth; wait on it instead of sleeping.
Use only help-confirmed options; on a usage error, read help, don't guess.
Flag possible regressions with a complete JSON report: evidence/errors/schema.
For blocking operations, finish in one call instead of polling across model turns.

Use recorded actions for live mutations (declare every changed resource); never hand-write receipts. One wrapper; client-go reads; source repairs are patches (`sdo-operator patch create --owner responder --reason ...`, see sdo-patching), never commits of app files. Restore namespace drift to accepted source unless live evidence establishes an intended difference; uncertainty alone doesn't block it. Explain benign drift with live evidence. When an admission control blocks recovery and its backend is gone, restore the backend if you can. Otherwise a scoped fail-open or removal that restores availability is acceptable; report it. CA: `SyncWebhookCA` ([ref](webhook-ca.md)). Secret data: `drift.unexplained` findings and a program's `DiffAgainstSource` on a Secret name only the keys whose live value differs from, is missing from or is not declared by accepted source; restore them with `RestoreSecretFromSource` ([fixes](fixes.md#built-in-actions)), never by reading or writing values. Repairs stay on app objects; never widen a selector or policy to other namespaces.

A `secret-reader-skew` or `secret-readers-predate-change` finding names a Secret whose data changed (by metadata; SDO never reads values) after the listed stale readers started, so they may hold an older value than the fresh readers. Decide which version is intended before restarting readers: if the change itself is wrong, restarting spreads it.

Replacing the Pods of a `holds_data` workload in the application map can lose its data when it has no persistent storage, and health probes that write their own data will not notice. Look for a cause that keeps those Pods first; if you must replace them, do it deliberately and say so in your report. Verification lists any such replacement in `data_at_risk[]`, and SDO then records the outcome as unconfirmed because healthy probes cannot show the data survived; do not recreate only what the probes read. A command, script, Job or HTTP call that reaches a `holds_data` component is recorded, not refused: verification lists its data effect in `data_effects[]` and the outcome keeps it as evidence. The typed [store data](store-data.md) actions record more (counts, backups, source state); fix the authoritative source before invalidating a cache, and name the writes a restore or repair may lose.

Webhook menu/program repairs automatically requeue affected ReplicaSets with admission `FailedCreate`, avoiding controller retry backoff. Repair admission prerequisites before dependent workloads. Pending requeues and unchanged objects do not reserve desired state; actual pending desired-state writes still conflict. After a raw admission repair, use `RetryAdmission` for the affected ReplicaSets in the incident namespace. It preserves replicas and Pod templates; its live readback proves the requeue, and independent verification must still establish recovery.

Saved repairs share a bounded recovery observation before handoff. A pending or timed-out observation does not mean its write failed: keep useful writes, inspect named blockers and residual findings, and avoid replaying them. Independent verified recovery can settle the original application later, including while a responder only observes.

Application receipts retain `settlement_rejection_reason` when independent
recovery cannot attribute success. This closure veto is separate from earlier
scoped verification facts. It identifies missing history, incomplete execution,
superseding writes or outstanding observation proof; it grants no retry or
mutation authority. Confirmed removals settle through fresh absence evidence.
