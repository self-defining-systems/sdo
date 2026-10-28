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

See [deployment configuration](../../../../docs/provider-capacity.md) for the
shared eight-session Opus pool. Missing pool configuration blocks Opus rather
than creating a private per-host pool. Older images and non-SDO CLI sessions
are outside this admission path.
