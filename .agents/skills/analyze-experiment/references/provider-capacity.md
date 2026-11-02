# Provider capacity evidence

Check provider capacity before attributing a failed row to SDO.

- `AgentResult.failure_reason=rate_limited` and the curated
  `ProviderRateLimited: rate_limited: ...` worker diagnostic identify provider
  infrastructure, never an SDO-quality failure.
- Fastloop rows write `grade_status=NOT-GRADED`,
  `not_graded_reason=rate_limited`, and `infra_error=rate_limited` when capacity
  prevents completion. Exclude these rows from both successes and failures.
  Recovery still runs; its independent cleanup oracle is not an incident grade.
- Exported `provider-waits.jsonl` uses `sdo.provider-wait/v1`, with `turn_id`,
  `incident_id`, `phase`, `kind` (`rate_limit` or `concurrency`), and `state`
  (`started`, `finished`, `exhausted`). Starts/finishes share `wait_id`; exhausted
  events carry `reason=rate_limited`. A start without a finish retains a killed
  worker's infrastructure cause. The file contains no model/tool output.
- Incident rows retain `provider_waits` intervals. Separate
  `rate_limit_wait_seconds` and `concurrency_wait_seconds` are clipped to the
  injection-to-resolution window. TTM subtracts the union only through the last
  successful mitigation. `raw_injection_to_mitigation_seconds` and timestamps
  preserve wall time. Do not subtract reflection waits or a peer incident's
  waits from a cumulative runtime export.
- Pipeline cost analysis subtracts provider waits from TTM and its last-command
  floor, and subtracts overlapping judge/provider waits once. `run_validity`
  treats exhausted/interrupted provider capacity as `invalid_infra`.
- Claude retries start at 30 seconds and double to 600 seconds, with 1800 seconds
  of waiting at most. Frozen outer deadlines still apply; a deadline consumed
  by provider waiting also makes the affected row ungradable.

- Fastloop manifests and rows retain `opus_pool: {mode, host_id, capacity}`.
  In explicit `per_host` mode, `capacity` is that host's static share (default
  four); sum the distinct hosts' shares, not the rows or lanes. Two hosts at
  four each reserve eight account sessions. All apps on a host must use the
  same lock inodes, including through different extraMount aliases. An idle
  share is never borrowed. Older rows without `opus_pool` provide no allocation
  evidence; launcher settings describe configuration, not proof of live adoption.

See [deployment configuration](../../../../docs/provider-capacity.md) for the
shared or static per-host Opus pools. Missing pool configuration blocks Opus;
per-host mode also refuses unprovisioned token files. Older images and non-SDO
CLI sessions are outside this admission path.


In `per_account` mode, `opus_pool.capacity` is the concurrency of each account,
not a host-wide share. Rows add `account_events` from the dedicated session
telemetry: account alias, selected/failover/exhausted state, typed reason, phase,
incident/turn IDs and timestamp. Reasons include primary/fallback, auth failure,
unavailable credential, registration disablement, measured quota exhaustion and
rate limiting. Responder and reflection switches are both retained; only responder
wait intervals reduce TTM. No tokens, emails or CLI streams are account evidence.

`account_exhausted` is infrastructure and makes the final row `NOT-GRADED`,
including reflection exhaustion. Preserve independently verified closure/grade
checkpoints from before reflection as evidence; use the final row's infrastructure
exclusion for denominators. The host's shared `account-state.jsonl` carries one
stable transition ID per account health change, with crash deduplication. Known
five-hour/seven-day quota exhaustion starts in cooldown; setup-token usageAPI403
is unknown eligibility and never an auth failure. Consult actual CLI results.
