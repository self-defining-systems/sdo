# Claude provider capacity

Every production Claude turn recognizes Claude Code's OAuth limit line and API
rate-limit/overload errors. It retries after 30 seconds, doubling to a maximum
delay of 10 minutes, with at most 30 minutes of provider waiting per turn.
Named sessions resume; completed work and accounting survive the retry. A retry
releases its Opus token before backoff. Other CLI errors retain their ordinary
failure behavior.

Opus turns (including an unspecified Claude model, whose CLI default may be
Opus) require an explicitly configured shared lock pool. Capacity defaults to
eight and is configured with `SDO_OPUS_MAX_SESSIONS`. Every host, lane and role
using the account must use the **same filesystem directory** and capacity.
An inconsistent capacity is rejected; there is no per-lane or local fallback.
The directory must support cross-client `flock` locks, including NFS lock
recovery. Keep its lock files in place: unlinking a locked inode breaks exclusion.
Drain all clients before changing capacity.

For host workers, set `SDO_OPUS_POOL_DIR` to the already-mounted shared
directory. For Kubernetes workers, set both `SDO_OPUS_POOL_NFS_SERVER` and
`SDO_OPUS_POOL_NFS_PATH` in the installer environment. The path is the server's
absolute export path, not the host's local mount path. Controller, operate and
every worker Job inherit these values and mount that export at `/sdo/opus-pool`.
Nodes need NFS client support and network access to the same server on both
hosts. Provision directory and file permissions for the worker UID (65532) and
any host callers; these files contain only capacity/lock metadata. Never put
credentials in this directory. A missing or unavailable pool blocks Opus before
starting the CLI.

Capacity waits are recorded independently in `provider-waits.jsonl` beside the
durable incident session, or beside `SDO_TURN_USAGE_LOG`; an explicit
`SDO_PROVIDER_WAIT_LOG` overrides that location. Records contain only IDs,
phase, timestamps, wait kind and state. They contain no CLI output or environment
dump. `started` is flushed before waiting and `finished` afterward, so a worker
killed during a wait retains its infrastructure cause. Successful turn usage
records also report `rate_limit_wait_seconds` and `concurrency_wait_seconds`.

Benchmark adapters own grading interpretation. Exhausted capacity produces
`AgentResult.failure_reason=rate_limited` or the same typed curated worker
failure. Fastloop marks the row `NOT-GRADED` with `infra_error=rate_limited`,
skips its incident oracle/diagnosis grading, retains cleanup, and excludes the
row from success/failure denominators. A hard outer deadline interrupted by
provider waiting is also ungradable; retries never authorize extending frozen
controller deadlines. TTM excludes the union of responder wait intervals clipped
to its measurement window. Raw timestamps and raw TTM remain available;
reflection and peer-incident waits are not subtracted. Pipeline analysis also
avoids subtracting overlapping judge and provider waits twice.

These controls apply to SDO's structured-turn path. Other CLI users sharing the
subscription must use the same pool or reserve capacity separately. Existing
running images do not acquire tokens until rebuilt/restarted with this code and
the shared export configuration.
