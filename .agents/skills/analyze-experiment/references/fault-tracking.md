# Fault tracking evidence

Fastloop incident schema `sdo.fastloop-incident/v2` requires a discriminated `injection`
object: `{state: injected, started_at, finished_at}` from the worker injection event
(the same request/reply captured in the audit), or `{state: not_injected, stage}`.
Stages: `pre_injection` (setup/baseline/RBAC stopped before the callback)
or `injection` (callback attempted without a returned worker event).
`not_injected` means no event is available, including remote timeouts with uncertain
writes. Never infer occurrence from local clocks.
Invalid injection with a recorded event remains `injected` and `infra_error=injection_invalid`;
validity and occurrence are separate facts. Composite event times span the first
recorded component start through the last recorded component finish, including
partial failures. Top-level `injection_started_at` and `injection_finished_at` are
derived columns only; both are null for `not_injected`, as are injection latencies.
Summaries count non-injected rows as infrastructure outcomes, exclude them from
incident success/latency denominators, and retain their error and wall time.
Sequence analysis excludes non-injected and unverified legacy rows. Historical
v1 timestamps, including microsecond pairs from setup failures, are not evidence:
reconcile against the worker audit before upgrading. The v2 loader rejects legacy rows.

Fastloop freezes the incident oracle and any inline diagnosis grade in
`incidents.grades/<six-digit-index>.json` beside `incidents.jsonl`, before collecting the
terminal learning receipt. Each checkpoint has the ordinary incident-record
schema but no completed reflection/recovery fields. Use it when a slow or failed
reflection has delayed the final row; do not count it as a terminal strict receipt.
The final row and checkpoint expose `graded_at` (alias of `oracle_finished_at`),
`closure_to_grade_seconds` (`graded_at - resolved_at`) and
`responder_runtime_seconds` (`responder_finished_at - responder_started_at`);
missing endpoints yield null. Deferred diagnosis grades arrive later in the final
rows and are not retroactively inserted into the immutable oracle checkpoint.
Oracle failure still reaches reflection using the original `AgentOutcome`, with
no grade feedback. Cleanup and the next injection retain the learning gate.

Per-fault tracking descriptions now retain `targets` alongside the existing
comma-joined `component` identity. A multi-target fault has one timeline entry and
clears only when every target probe is green. Field-restoration probes capture
only their relevant healthy live fields before injection and then require their
restoration plus a current available rollout. A different valid repair may stay
unresolved, and a peer fault on that workload can delay this conservative time.
Flag probes read the specific flag in the application's ConfigMap; admission
probes measure the blocked workload's current replica recovery. These observations
are independent of official verdicts and must never be described as grader proof.
Use `fastloop check-plan --plan <plan-or-stream.json>` to reject any unprobed
selection offline; permissive repair capabilities do not relax this tracking gate.
