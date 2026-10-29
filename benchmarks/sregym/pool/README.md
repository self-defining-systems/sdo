# Cross-host real-app pool

The run owner uses one lane registry and one pair journal across both hosts.
`python -m benchmarks.sregym.pool run --config /outside/repo/pool.json` owns the
run; `lanes --registry ...` reports its lanes. Processes run on the lane's host
under its coord lease and systemd unit. Keep run files outside this repository.

## Fresh memory and images

For each app, run `render-seed --code-root CODE --source SOURCE --destination SEED
--application APP --namespace NAMESPACE` from the exact frozen build. APP is
`hotel-reservation`, `social-network`, or `opentelemetry-demo-astronomy-shop`.
SOURCE supplies accepted source inputs only. The renderer discards its memory
and runtime data, selects the repository-owned health/traffic template, renders
the accepted manifests, and regenerates generic health detectors with this
build's Go memory API. It records build, source, seed and render identities in
the seed's Git directory. No run19 memory is imported.

Prepare a shared registry with all content-identified app images before starting
lanes. Pool setup sets `FASTLOOP_APP_IMAGE_PULL_ONLY=1`: even a local cached image
requires a manifest in the named shared registry, and a lane never builds or
publishes app images. A missing image fails preparation with its exact ref.

Each host's optional `registry_relay` config names a systemd service and its
listen/upstream host/port. The registry host exposes its loopback registry on
the private LAN; peers relay from loopback to that listener. Use distinct owned
ports while retiring hand-started relays. The owner checks every 30 seconds;
systemd restarts failed relays; enabled units also start with the user manager (reboot startup requires host linger). The
health check requires readiness from the owned service PID plus `/v2/` through
the entire transport. Docker is never restarted. The host user manager must be
available on both hosts.

## Real lane preparation

Write a `RealAppSetup` JSON per app/host with `code_root`, `seed`, `seed_revision`,
`registry`, `images` (`controller`, `responder`, `validator`), `credentials_hook`
and `cpu_limit` (per node, default 2; the lease books both nodes). The seed
receipt must match both its HEAD and the frozen code HEAD.

Use this pool `prepare_steps` command (each word is one array element):

```text
{python} -m benchmarks.sregym.pool prepare-real-app
  --setup /outside/repo/setups/{app}.json --run-dir {run_dir}
  --lane {lane} --problem {problem} --lease {lease}
  --repo {repo} --branch {branch}
  --provider {provider} --model {model} --reasoning-effort {reasoning_effort}
```

Setup deploys from the shared registry, provisions the existing credential
Secret with the hook, runs the real no-fault preparation path with permissive
capabilities, binds through `remote_memory.bind_lane_to_branch`, and proves
fetch/push from the controller using that host's mounted GitHub key. Endpoint
configuration remains `{app: {repo, branch}}`, one explicit non-main branch per
app. Seeding is from fresh rendered memory, and later lanes may join only a
matching tree from that branch's history. The preflight uses its own temporary
Git repository and deletes its temporary key copy; it never fetches into the
broker's accepted checkout.

The pool defaults to `provider: claude`, `model: claude-opus-5-5`,
`reasoning_effort: medium`. One run setting configures preparation and every
worker role, including reflection. `agent_registrations` selects scripted
registrations for a no-LLM rehearsal; omit it for live workers. Use a live
responder image for both modes and switch registrations on an idle lane. The
pool pauses the controller, replaces only its registration ConfigMap, restarts
the existing controller Job, and retains its accepted repository and upstream.
Claude Opus 5.5 requires CLI >=2.1.280.

Set `credentials_hook` and `credentials_keeper` to the established host scripts.
The hook is also passed to each encounter; the keeper is a child of the pair
unit, uses a lease-fenced kubectl shim and is stopped when the pair ends. Fresh
Python processes run through `lane_guard` so lease expiry/transfer stops later
operations. Encounters use permissive stream capability mode and the shared
fastloop command's one cleanup pass. Configure RATE1's identical `SDO_OPUS_POOL_NFS_SERVER`, `SDO_OPUS_POOL_NFS_PATH`
and capacity on both hosts before Opus preparation. The pool installs NFS client
helpers on its own kind nodes when needed; it never changes the host Docker
configuration. Capacity exhaustion remains an infrastructure result.

After the final pair, run `sync-memory --run-dir LANE --code-root CODE --lease LEASE`
on each host. This invokes the production broker sync under its existing lock,
then records `memory-convergence.json` only when the controller and GitHub heads
agree. It never creates a synthetic incident or imports grader evidence.

For a balanced rehearsal, `max_pairs_per_lane: 2` lets a ready lane start
immediately and limits it to two completed problem pairs while a slower peer
prepares. Completed lanes keep their cores booked. Leave the setting unset for
normal streams with unlimited lane reuse.

A retry containing only the repeat stays on the lane that completed its first
encounter. If that lane is busy, held, over its load limit or on another build,
the repeat waits; a less-loaded peer cannot change the pair's lane identity.
