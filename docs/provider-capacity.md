# Claude provider capacity

Every production Claude turn recognizes Claude Code's OAuth limit line and API
rate-limit/overload errors. It retries after 30 seconds, doubling to a maximum
delay of 10 minutes, with at most 30 minutes of provider waiting per turn.
Named sessions resume; completed work and accounting survive the retry. A retry
releases its Opus token before backoff. Other CLI errors retain their ordinary
failure behavior.

Opus turns (including an unspecified Claude model, whose CLI default may be
Opus) require an explicitly configured lock pool. `SDO_OPUS_POOL_MODE=shared`
(the default) uses one account-wide filesystem directory, with eight tokens by
default. `SDO_OPUS_POOL_MODE=per_host` assigns a static share to each host,
defaulting to four tokens per host (4 + 4 = 8). `SDO_OPUS_MAX_SESSIONS` sets the
pool's capacity in either mode. All roles and lanes on a host share its tokens;
the run owner must ensure that all host shares sum to at most the account cap.
An inconsistent capacity is rejected. There is no automatic local fallback,
borrowing of idle shares, or per-lane pool.
The turn boundary also rejects pool paths inside the agent checkout, including
symlink aliases, before creating metadata or starting the CLI.
The directory must support cross-client `flock` locks, including NFS lock
recovery. Keep its lock files in place: unlinking a locked inode breaks exclusion.
Drain all clients before changing capacity.

For host workers in shared mode, set `SDO_OPUS_POOL_DIR` to the already-mounted shared
directory. For Kubernetes workers, set both `SDO_OPUS_POOL_NFS_SERVER` and
`SDO_OPUS_POOL_NFS_PATH` in the installer environment. The path is the server's
absolute export path, not the host's local mount path. Controller, operate and
every worker Job inherit these values and mount that export at `/sdo/opus-pool`.
Nodes need NFS client support and network access to the same server on both
hosts. Provision directory and file permissions for the worker UID (65532) and
any host callers; these files contain only capacity/lock metadata. Never put
credentials in this directory. A missing or unavailable pool blocks Opus before
starting the CLI.

For **per-host pools**, use an existing writable kind extraMount; lanes need
not be recreated. Provision the tokens on the host first, outside source
checkouts. If different apps mount different host roots at `/opt/sdo-parallel`,
give each root an alias with the same lock inodes. From the run's code root:

```bash
(
  umask 000  # Values-free files must be writable by worker UID 65532 and host callers.
  uv run python scripts/prepare_opus_host_pool.py \
    --directory /absolute/host/opus-pool --capacity 4 \
    --alias /existing/hotel-parallel-root/opus-pool \
    --alias /existing/social-parallel-root/opus-pool
)
export SDO_OPUS_POOL_MODE=per_host
export SDO_OPUS_POOL_HOST_ID=chelan5
export SDO_OPUS_MAX_SESSIONS=4
export SDO_OPUS_POOL_HOST_PATH=/opt/sdo-parallel/opus-pool
unset SDO_OPUS_POOL_NFS_SERVER SDO_OPUS_POOL_NFS_PATH
```

Run preparation once on each host, with that host's mounted roots and unique
host ID. The helper is idempotent, refuses an independent alias or a different
capacity, and never replaces existing files. Hard-link aliases must be on the
same filesystem as their canonical host pool. Add every participating app root;
creating independent pools under each root would multiply the host share.
Provision while drained and retain the inodes for the run's lifetime.

Pass these settings to both installer and fastloop run environments. The
installer uses a `hostPath` of type `Directory` (never `DirectoryOrCreate`) at
the existing node path and mounts it at `/sdo/opus-pool` in controller, operate
and all workers. Host-side workers instead set `SDO_OPUS_POOL_DIR` to the
canonical host directory. Per-host workers require all token files to exist
before admission and never create them. A lane without the selected existing
extraMount cannot participate until it has an explicitly verified pool path;
do not substitute a node-private directory. Fastloop's manifest and every row
record `opus_pool: {mode, host_id, capacity}`; `capacity` is the host share in
per-host mode. Older rows have no allocation evidence.

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
the chosen pool configuration.


## Per-lane registered accounts

`SDO_OPUS_POOL_MODE=per_account` selects host-local cswap accounts instead of the
legacy default login. This mode applies to every Claude role and model. Every
lane receives a primary alias; healthy accounts are assigned round-robin, giving
each lane a distinct primary when enough accounts exist. A configured lane never
falls back to an unmanaged default login. Legacy modes keep their existing login
and capacity behavior. Initial rollout is restricted to chelan4 astronomy lanes;
chelan5 keeps its current default login until separately authorized. Do not export
or copy account credentials between hosts.

Run `scripts/prepare_claude_accounts.py` on the account's own host. It captures
`cswap list --json`, reads Linux cswap's base64 credential store privately, and
writes a private bundle outside source checkouts. Only aliases, health, lane
assignments and capacity reach stdout. Measured exhausted five-hour/seven-day
windows exclude accounts until their reset. An unavailable usage API, including
setup-token HTTP403, leaves eligibility to actual model-call results. No current
unmanaged login is imported. Missing/corrupt individual credentials are disabled
without hiding healthy peers.

```bash
# On the authorized account host only. All paths are host-local references.
(
  umask 000  # Only values-free pool/state files need shared worker write access.
  uv run python scripts/prepare_claude_accounts.py \
    --directory /private/claude-accounts --pool /host/account-pool \
    --alias /existing/astronomy-parallel-root/account-pool \
    --lane 50 --lane 53 --lane 54 --capacity 2 \
    --slot 2 --slot 3 --slot 4  # Optional: only these cswap slots are assignable or failover targets.
)
export SDO_OPUS_POOL_MODE=per_account
export SDO_OPUS_POOL_HOST_ID=chelan4
export SDO_OPUS_POOL_HOST_PATH=/opt/sdo-parallel/account-pool
export SDO_OPUS_POOL_DIR=/host/account-pool
export SDO_CLAUDE_ACCOUNTS_SECRET=sdo-claude-accounts
export SDO_CLAUDE_ACCOUNT_PRIMARY=a02  # Use this lane's emitted mapping.
export SDO_CLAUDE_ACCOUNT_MAX_SESSIONS=2
export SDO_CLAUDE_ACCOUNT_BACKOFF_SECONDS=5
export SDO_CLAUDE_ACCOUNT_COOLDOWN_SECONDS=300
unset SDO_OPUS_POOL_NFS_SERVER SDO_OPUS_POOL_NFS_PATH
```

Create/update the separate Secret in each lane's control namespace using
`kubectl create secret generic sdo-claude-accounts --from-file=/private/claude-accounts
--dry-run=client -o json | kubectl apply -f -`, with the correct context/namespace
on both commands. Never use literal credentials or print the manifest. The Go
installer projects the whole Secret read-only at `/sdo/claude-accounts`, without
`subPath`, into controller, operate and cold/warm worker Pods. Each attempt reopens
the inventory and credential reference, so updating the Secret refreshes workers
without restarting a lane. Keep the Claude config/session home unchanged across
accounts; only the child process OAuth environment changes. Competing API keys,
custom headers and alternate Claude backends are cleared; OAuth uses Anthropic's
canonical API endpoint.

`SDO_CLAUDE_ACCOUNT_MAX_SESSIONS` sets concurrency **per account**, default two;
`SDO_OPUS_MAX_SESSIONS` does not control this mode. Provision account lock/state
inodes once, with hard-link aliases for every existing app extraMount. Workers
never create a private account pool. Drain all consumers before resizing account
capacity. The credential bundle remains private (directory0700/files0600); the
pool contains no credentials. Host workers additionally set
`SDO_CLAUDE_ACCOUNTS_DIR=/private/claude-accounts`.

Authentication failures, including real CLI401/403, expired/revoked tokens and
invalid-key/login diagnostics, disable that registration until human
re-registration. A rate/overload retries once after the short backoff, then cools
down that account and tries the next healthy one. Busy healthy accounts wait
within the existing bounded admission budget. Tool output mentioning auth/limits
is not a provider failure. Named sessions, ownership, tool audits and accounting
survive the switch. Health is journaled by registration so stale Secret versions
and torn writes cannot revive a disabled token. cswap add/add-token re-registration, including the same value, creates a new
opaque registration ID. Ordinary OAuth refresh preserves the quarantine.

Every selection/skip/failover is recorded by alias and typed reason in session
`provider-waits.jsonl`; rows retain `account_events` for responder and reflection.
Shared `account-state.jsonl` is the orchestrator's values-free state-change outbox,
one stable, crash-deduplicated post per transition. Read it on the account host;
never copy the private bundle for monitoring. No healthy account yields typed
`account_exhausted`, final `NOT-GRADED`, and exclusion from SDO success/failure
counts. A pre-reflection closure grade stays immutable evidence if exhaustion
occurs during reflection; the final row carries the infrastructure exclusion.
