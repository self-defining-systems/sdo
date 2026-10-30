# Live repairs

Use recorded actions for live mutations (declare every changed resource); never hand-write receipts. One wrapper; client-go reads; commit source repairs. Restore namespace drift to accepted source unless live evidence establishes an intended difference; uncertainty alone doesn't block it. Explain benign drift with live evidence. When an admission control blocks recovery and its backend is gone, restore the backend if you can. Otherwise a scoped fail-open or removal that restores availability is acceptable; report it. CA: `SyncWebhookCA` ([ref](webhook-ca.md)). Repairs stay on app objects; never widen a selector or policy to other namespaces.

Webhook menu/program repairs automatically requeue affected ReplicaSets with admission `FailedCreate`, avoiding controller retry backoff. Repair admission prerequisites before dependent workloads. Pending requeues and unchanged objects do not reserve desired state; actual pending desired-state writes still conflict. After a raw admission repair, use `RetryAdmission` for the affected ReplicaSets in the incident namespace. It preserves replicas and Pod templates; its live readback proves the requeue, and independent verification must still establish recovery.

Saved repairs share a bounded recovery observation before handoff. A pending or timed-out observation does not mean its write failed: keep useful writes, inspect named blockers and residual findings, and avoid replaying them. Independent verified recovery can settle the original application later, including while a responder only observes.

Application receipts retain `settlement_rejection_reason` when independent
recovery cannot attribute success. This closure veto is separate from earlier
scoped verification facts. It identifies missing history, incomplete execution,
superseding writes or outstanding observation proof; it grants no retry or
mutation authority. Confirmed removals settle through fresh absence evidence.
