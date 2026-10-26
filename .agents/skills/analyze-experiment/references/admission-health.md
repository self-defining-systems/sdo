## Pod admission fails while existing Pods remain Ready

**Signature:** generic health detector `pod-admission`, rule `pod-admission-rejected`, primary resource the application Namespace. Evidence is the API error from a server-side dry-run Pod create. Existing readiness does not establish that admission works: temporarily permissive admission can restore workloads without repairing the admission path. Rejection still blocks closure after that policy is restored. No webhook or fault identity is required. Network/timeout failures and missing observations are detector errors, never clearance. Captured snapshots carry the frozen `pod_admission` result for replay; older snapshots cannot establish admission health. The probe checks one deterministically selected active app Pod spec (or a minimal Pod if none exists), not every possible selector or admission identity.

Detector history status `pending` means an unconfirmed observation window. It is valid in durable ControllerState, incident requests and reflection/outcome inputs. It proves neither firing nor clear; exclude it from healthy/repair evidence and wait for confirmed evaluations. Preserve pending-to-clear/firing/error transitions when compacting history. A missing checkpoint accompanied by a pending-status validation error is controller persistence failure, not health evidence.

The bounded `--evaluate-once` health command reports `status: unavailable` and an `unavailable` list of detector IDs/reasons when service reachability has no prober. This command-level verdict is not persisted detector history and cannot establish onboarding, seed health or repair authority. `pending` still means an incomplete observation window. `--source-drift-report` returns inventory-only `healthy`/findings before probing; its healthy value is no claim about detector health, and combining it with `--accept-seed` is rejected.

Steady synthetic traffic uses `pending` when a scenario has never succeeded since observation began or lacks enough recent usable SLO samples. `DetectorEvaluation.reason` (protobuf field 6; `Reason` in the Go tool-state JSON) names unproven scenarios and is informational, separate from `error`. Partial qualification blocks aggregate health clearance; qualified failures still fire. Pending alone never dispatches an incident and is not independent recovery evidence. Qualification remains process-local: after a mid-incident restart, fresh successful steady samples and ordinary clearing persistence restore closure eligibility. `sdo verify` reports the controller's verdict, not the burst aggregate alone. Preserve reason transitions during history compaction.


`fastloop eval-dry-run --run-dir <lane>` writes `eval-dry-run/report.json` with per-invariant pass/fail, elapsed seconds, and explicit `fault_injected:false` / `live_model_calls:false`. It executes the real pre-injection path using scripted registrations. A five-minute process deadline marks unfinished invariants failed; this evidence establishes eval plumbing, never learning. Admission evidence includes setup lineage, exact responder RBAC against the real installer plan, current controller detector states, qualified native prober scenarios, independent traffic and stable workload identity. Stabilization is recorded before the incident injection window and excluded from incident clocks. Preserve the report and the `admission/` evidence when a slot is rejected.

## Lane resource evidence

`admission/lane-resources.json` records namespace/Pod/container working-set
measurements and budgets (default 8GiB; observer and app all-in-one Jaeger
2GiB), host available memory, and `host_disk` evidence for the filesystem
backing local Docker kind-node storage. The disk object names `mount`,
`storage_path`, `total_bytes`, `free_bytes`, `used_percent`,
`min_free_percent` and `floor_bytes`; default admission rejects at 90% used or
below 100GiB free. Checks run before and after stabilization; the dry run
reports the same `lane_resources` invariant. Resource rejection is an eval
infrastructure failure, never evidence of agent or incident mitigation.
Only the benchmark observer is capped by SREGym: app growth is detected,
and application workload documents are preserved from accepted source.

New prepared-lane captures seal `fastloop_application.namespaces[].objects` with `installed_digest`. This installed set takes precedence over an inherited seed lock while source is unchanged; accepted source repairs restore from the current lock. Digest mismatches reject loading. Dry-run receipts include `code_revision` and per-stage resource, restoration, RBAC, setup and traffic results.

Dry-run timing separates one-time setup (`setup_seconds`, at most900s) from steady admission (`steady_seconds`, at most300s). Native onboarding plus the first complete detector observation ends setup; every existing health, traffic, lineage, placement and closure check remains mandatory during steady proof.

Host disk admission uses an absolute free-byte floor (default100GiB) on the Docker storage mount. The receipt names `rule: absolute-free-bytes`, the measured free bytes and floor; used percent is informational. A large volume with hundreds of GiB free must not fail solely on percentage. For historical comparisons retain the previous percent-and-byte rule and its rejection as eval evidence.
