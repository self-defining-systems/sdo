# Simulation fidelity

What the Go deterministic simulation models, what it deliberately does not, and the ledger of alignment
gaps between it and real Kubernetes. The [align-simulation skill](../.agents/skills/align-simulation/SKILL.md)
describes how gaps are found and closed; every closed or accepted gap updates this file.

## Modeled

The simulated API server (`sdo/operator/runtime/sim`) models:

- server-side defaulting, quantity normalization and resourceVersion preconditions on patches;
- status-only resourceVersion churn between a caller's read and its patch;
- a patch that commits and then returns a transport error (outcome unknown to the caller);
- informer caches that keep serving the pre-write object for a seeded number of reads;
- a `testing/synctest` bubble: controller timers, injected clocks and sleeps share virtual time;
- one seeded RNG per run, consumed in call order, so a failing seed replays (`SDO_SIM_SEED`);
- all snapshot resource kinds, namespace isolation and defensive ordered object reads;
- explicitly scheduled pod backoff/recovery, aggregated BackOff events and deployment readiness lag;
- raw kubelet summary/cAdvisor samples and timestamped current/previous container logs through an
  in-memory HTTP RoundTripper, consumed by the real snapshot, usage and logs collectors;
- terminated-container tail retention through the production logs observer;
- HTTP, gRPC health, Redis, Memcached, MongoDB, Postgres and MySQL handshakes over `net.Pipe`;
- scripted API errors, commit-then-timeout, stale cache reads, dropped cache updates until relist,
  delayed/failing kubelet/log/probe reads and per-edge partitions;
- memory-host Evaluate/Diagnose/Mitigate over in-memory gRPC (bufconn), frozen
  snapshot/input parity, host disconnect errors and host-owned log receipts using the same simulated API;
- memory-host two-commit activation through Manager, detection during injected
  compilation delay, changed program plans, retained finding state, candidate
  failure, leased-call retirement, complete mirror deltas and same-revision crash recovery;
- the Kubernetes API a client sees over HTTP, for the 20 kinds the snapshot reads (see
  [Kubernetes conformance](#kubernetes-conformance)): get, list (name order, label and field selectors, metadata-only
  `Accept`), create, update, merge, strategic-merge, JSON and server-side apply patches (real ownership through the
  server's own field manager and its OpenAPI schema, conflicts, `force`, `dryRun`), delete (preconditions,
  finalizers, `deletionTimestamp`), the `status` and `scale` subresources, `metadata.generation`, resourceVersion
  ordering, watch (replay after a version, then live; optional delivery lag; a watcher that falls 1024 events behind is cut
  off), Leases (leader election), RBAC Roles, RoleBindings, ClusterRoles and ClusterRoleBindings (the state
  tracker lists them), discovery, and the defaults the registry and admission fill in
  for Pods, Services, Deployments, StatefulSets, DaemonSets, ReplicaSets, Jobs, PVCs, NetworkPolicies, Secrets and
  Nodes;
- registered invariant hooks and a 45-second host-time seed-sweep budget;
- an ordered event log (`Recorder`) of controller writes, observations, detector evaluations, incident lifecycle, and memory activations, with the two behavioral checks over it, every incident ends and detection keeps running (`sim/properties.go`);
- the memory broker's independent compile gate (`sim.Compiler`): it records each call, honors cancellation and
  rejects changed Go files that do not parse, with an injectable typed verdict. It does not type-check, link or run
  detector tests.

The pure lifecycle core (`runtime/lifecyclecore`) is covered by exhaustive explorers of closure and
reflection under bounded API faults, restarts and worker attempts (see `docs/lifecycle-state-machine.md`).

## Kubernetes conformance

`sdo/operator/runtime/sim/conformance_*_integration_test.go` runs one table of small scenarios (`TestKubeConformance`)
against a real kube-apiserver and etcd (controller-runtime `envtest`, version 1.34.0 to match the kind node image) and
against the simulated API, and requires the observable results to match: status code, reason, error causes, and the
normalized body, including `managedFields`. Each scenario is a list of HTTP-level steps, so the simulation is tested
on the wire the controller and memory host use (the accepted-source client, plan executor, informers and discovery).

Run it: `go test -tags=integration ./sim/ -run TestKubeConformance` in `sdo/operator/runtime` (about 20 s once the
binaries are cached; the first run downloads them through `envtestio`, see the testing guide). PR CI runs it in the Go job, with the binaries
cached by Kubernetes version. `SDO_CONFORMANCE_DUMP=<dir>` writes both sides' normalized results
per scenario. After a Kubernetes version bump, `SDO_REGEN_SIM_SCHEMA=1 go test ... -run TestRegenerateSimSchema`
refreshes `sim/testdata/openapi-subset.json`, the real server's OpenAPI v3 schema (descriptions removed) for the
served kinds, which the simulation's server-side apply needs for list-map keys and field ownership.

A scenario may list `Accepted` differences, each with its reason; a listed difference that no longer differs fails the
test, so the list cannot go stale. To add behavior to the simulation, add a scenario first and watch it fail.

| Area | Scenarios |
| --- | --- |
| Server defaults | `defaults/*`: Service (ClusterIP, headless, NodePort, ExternalName), Deployment (strategy, probes, env, volumes, quantities), Pod, ConfigMap, StatefulSet, Job |
| Server-side apply | `ssa/*`: create, no-op, change, conflict and force, field removal, disjoint owners, list-map merge by key, update-owned field taken by apply, dry-run apply, dry-run create default render, acceptance record, validation errors |
| Patch and update | `patch/*`, `update/*`: merge-patch null and list semantics, no-op keeps the version, strategic merge, JSON patch, resourceVersion and uid preconditions, missing object, unsupported type, PUT versions, duplicate create |
| Status and generation | `status/*`, `scale/*`: generation rules, status and scale subresources, main resource ignores status |
| Delete | `delete/*`: preconditions, dry run, finalizers (terminating, adding a finalizer, release), pod delete and recreate identity |
| Leases and RBAC | `rbac/*`: Role, RoleBinding, ClusterRole, ClusterRoleBinding create, get, list, patch, delete. `lease/*`: create, renew with the right and a stale version, release, takeover, duplicate create, watch from a list version |
| List and watch | `list/*`, `watch/*`: name order, label and field selectors, list version at least its items', replay after a version, initial state, selected watch, status writes and finalizer deletion in the stream |
| Discovery | `discovery/*`: `/api`, `/apis`, group-version resource lists for the served groups |
| Other | `cluster-scoped/node`, `metadata-only/secret-reads` |
| Bounds | `bounds/*` keep the accepted differences below visible as scenarios |

### Conformance gaps found and closed (2026-11-03)

The simulation answered only list GETs, discovery and two dry-run forms over HTTP, and everything else through the Go
`Kube` object, so its behavior was never compared with a server.

| Class | Gap | Resolution |
| --- | --- | --- |
| Missing behavior | Server-side apply was a JSON merge patch: lists were replaced whole, no ownership, no conflicts, no `force`, no `managedFields`, so source comparison and the drift code that reads `managedFields` ran on data a real server never produces | Real field manager and OpenAPI schema: `ssa/*` |
| Missing behavior | Defaults existed only for Services (and part of Deployments); Pods, StatefulSets, DaemonSets, ReplicaSets, Jobs, PVCs, NetworkPolicies, Secrets and Nodes were stored as written | Defaults and registry work added: `defaults/*`, `cluster-scoped/node`, `metadata-only/secret-reads` |
| Mock encodes wrong behavior | Defaults that registry or admission set after ownership is recorded (cluster IP, IP families, node ports, Pod priority and tolerations, status) looked owned by the writer | Applied after the field manager runs: `defaults/*` managedFields |
| Mock encodes wrong behavior | The deployment-controller revision annotation and `observedGeneration` were stamped on every object, including Services and ConfigMaps | Deployments only (the controller manager is modeled, not the API server) |
| Missing behavior | `metadata.generation` was never set or bumped | Set to 1 on create for kinds that track it, bumped on spec change (and on annotation change for Deployments): `status/*`, `patch/*` |
| Wrong behavior | A recreated object got the same uid; a patch naming another uid was a conflict (real: 422 invalid) and any patch with a `metadata` key but no resourceVersion conflicted | Unique uids; invalid for a changed uid; conflict only for a stale resourceVersion present in the patch: `patch/preconditions-and-missing` |
| Missing behavior | List items carried `kind`/`apiVersion`, the list version was always 1, no selectors, Secrets were blanked instead of served as metadata-only objects | Real list shape, selectors, `PartialObjectMetadata`: `list/*`, `metadata-only/secret-reads` |
| Missing behavior | No delete preconditions, finalizers, `deletionTimestamp`, or Pod terminating object; no rule against adding a finalizer to a terminating object | `delete/*` |
| Missing behavior | No status or scale subresource, so a write to the main resource could change status and an autoscaler's ownership of `replicas` was not representable | Status split and scale entry: `status/*`, `scale/*` |
| Missing behavior | No watch | Replay-then-stream watch with the same event order and versions: `watch/*` |
| Wrong behavior | Writes carried no field manager, so ownership could not distinguish writers | Manager from `fieldManager` or the user agent; `World.ClientFor` sets the user agent |

### Conformance bounds (accepted)

| Bound | Reason |
| --- | --- |
| No object validation (name format, immutable fields such as a Deployment selector, required fields), `bounds/validation` | Validation is a large rule set that no controller decision reads; a refusal can be injected as a scripted API error. A kind lane catches a wrong manifest |
| Namespaces are not objects: a create in a missing namespace succeeds, `bounds/missing-namespace` | Worlds are built without Namespace objects; adding them would touch most simulation tests |
| Discovery lists only the served kinds, versions (`autoscaling/v1`) and the `status` and `scale` subresources | The controller resolves kinds it was configured with; other groups are not modeled |
| No controllers beyond explicit scheduled transitions: no garbage collection or `propagationPolicy`, no ReplicaSet, Endpoints or scheduler work, no graceful termination of scheduled Pods | The kubelet and controller manager are scenario inputs ([Not yet modeled](#not-yet-modeled)) |
| Watch has no bookmarks, no `410 Gone` or compaction, no `sendInitialEvents`; list has no `limit`/`continue`; no `deleteCollection` | No SDO code depends on them; client-go informers relist on error |
| Admission plugins other than Priority, DefaultTolerationSeconds, TaintNodesByCondition are absent (ServiceAccount token volumes, quotas, mutating webhooks) | Envtest runs with the ServiceAccount plugin off; webhooks are cluster specific |
| Cluster IPs and node ports are allocated in sequence, not from a CIDR or the 30000-32767 range | Controllers read presence and equality of these values, never their number |
| Timestamps are whole seconds; uid and generated names are deterministic | Deliberate: runs replay by seed |
| The `scale` subresource moves `spec.replicas` ownership by direct edit of `managedFields`, not through the server's scale handler | The result matches the server for the scenarios; other fields cannot be written through `scale` |
| OpenAPI schema is a snapshot of Kubernetes 1.34.0 for the served kinds | Refreshed with `SDO_REGEN_SIM_SCHEMA=1` when the kind image version moves |

## Controller harness (`runtime/harness_*_integration_test.go`)

`TestHarness` runs `RunWithOptions`, the production controller loop, on virtual time (a `synctest` bubble) with one
seed fixing the whole plan, and checks the two behavioral properties over the recorded log
(`sim/properties.go`): every incident ends, and every detector of the active memory revision is evaluated within
3 x its own interval plus 12 s. PR CI runs the registered seeds (`registerPRSeed`); the nightly run plays all 64, with `-race`.
A failing seed prints `SDO_SIM_SEED=<n> go test -tags=integration -run '^TestHarness$' ./runtime`.
`SDO_SIM_OUT=1` prints the controller's own output, `SDO_SIM_STATE=1` the final state ConfigMap, `SDO_SIM_DUMP=1`
the goroutines at the end, `SDO_SIM_AGENT=<mode>` forces every agent answer and `SDO_SIM_NOLAG=1` removes cache lag.

What a seed chooses: 2 to 4 revisions of 1 to 5 stand-in detectors (normal, error, panic, hang, slow, wrong
answer), swaps between them, when each fault object breaks, controller restarts, controller deaths at effect
boundaries (below), host crashes, the agent's answer to each dispatch (fix, nothing, worsen, crash, hang, garbage),
API errors, commit-then-timeout writes, get and informer list errors, expired and dropped watches, and watch lag.
All faults stop at quiesce; afterwards every detector behaves, the agent fixes, and the run lasts 150 s more.

What the checks add to the two properties: a controller that is still dying 30 s after faults stop is wedged (it
cannot hide in the downtime the detection check excuses; `Config.Restart`).

The cluster is `sim.Kube` behind its HTTP transport, the server `TestKubeConformance` compares with a real
kube-apiserver. Every client is a real client-go client; faults are a RoundTripper in front of the simulated server
(`harness_cluster_integration_test.go`), so they are wire events: an error status, a lost reply, a 410 for a watch
that resumes from a version the server no longer holds (the informer must relist), a watch connection dropped a few
seconds in, a list error. Watch lag is the simulation's own delivery lag. Fidelity changes, in order:

- **Cluster on `sim.Kube`.** The client-go fake clientset and its reactors are gone. Defaults, uids,
  resourceVersion preconditions, field ownership, Leases (leader election), RBAC kinds (the state tracker lists
  them) and watch all come from the simulated server. The sim gained a store lock (informers, the controller and the
  agent are concurrent clients), Lease and RBAC kinds, optional watch lag, and cut-off of a watcher that falls
  behind; each has a conformance scenario (`lease/*`, `rbac/*`, `watch/informer-list-then-watch`).
- **Informer list errors and expired watches.** Informer lists fail and resumed watches get 410. Not understood in the
  first harness, the old "informer factory blocks in `Shutdown`" was an artifact: client-go keeps a process-wide error
  throttle that remembers the wall time of the last handled error and sleeps the difference, and a bubble's clock starts
  in 2000, so the first list error slept for decades of virtual time. `disableErrorBackoff` removes that throttle (it
  only paces error logging).
- **Crashes are kills.** A controller crash is a dead process: every later request it makes fails, its open watches
  break, it records nothing, and the dispatcher stand-in refuses it. Its deferred cleanups still run but reach
  nothing, so the Lease is not released (the next process waits for it to expire) and nothing is flushed at shutdown.
  The next process starts from nothing but the cluster. Deaths happen at several effect boundaries, counted across
  restarts: before a control-namespace write lands, after it commits and before the reply, after a Lease write commits,
  during an informer list, as an incident is handed to the agent, and after the agent acted and before its answer is
  read. This is what exposed the state-restore crash loop on seed 1, which a graceful cancel hid.

Fidelity limits, each deliberate:

- Stale reads are watch events held back by a lag, a 410 relist and dropped watches; the sim does not serve a
  list older than the store (no stale list from a lagging apiserver cache).
- The agent stand-in sits at the `Dispatcher` boundary, with the Job dispatcher's own result validation applied
  (`validateIncidentResultFor`). It is not driven through the responder tool transport: that transport is a directory
  of request and response files under a repository PVC served by `KubernetesJobDispatcher` beside a Job executor, a
  worker registry, a memory repository and session directories, none of which the harness has; the tool handler
  (`ResponderLoop.Handle`) is only reachable through that dispatcher. The Job executor, broker, reflection and
  acknowledgement are not run, so an incident is terminal at `incident_closed`, and a closure that exhausts its retries
  is not exercised. Saved programs are driven: see below.
- The memory host is the real `memoryhost.Manager` with in-process gRPC hosts (bufconn). Process launch, limits and the
  build step are absent. A swap runs `Prepare`, `Activate` and `ReconfigureMemory` directly instead of
  `memoryActivation`, and does not rebuild the controller's snapshot provider. The swap
  takes the controller's observation lock (the one every pass holds) by polling `TryLock` on virtual time, so it lands between two passes as it does on the loop in production; a blocking lock wait would stop the bubble's clock. A host crash closes the active host and
  lets the manager's own recovery relaunch it. A hung detector is abandoned at the host's cooperative budget (the
  manager holds no lock across a detector call, so a hang delays only its own result). The host survives a controller
  death, as the separate host process does.
- A kill is delivered to the process's clients, not to its goroutines: Go cannot kill a goroutine, so a dead
  controller's goroutines run until the cancelled run context stops them, and the harness waits for `RunWithOptions` to
  return before starting the next process.
- `ControllerUp` is recorded when the harness relaunches the controller, so informer sync and lease takeover count
  against the detection bound (the 12 s slack). A detector the plan makes hang is exempt from its own bound only.
- A seed fixes the plan and the fault decisions are functions of (seed, call, count), but the goroutines inside the
  controller are not serialized, so replay is reliable, not bit-exact.
- Informer resync is set to 10 virtual minutes (`RuntimeOptions.InformerResync`) so client-go reflector timers stay in
  the bubble.
- **No process runs in a harness.** The accepted repository is a value: the observation scope and commit
  (`RuntimeOptions.AcceptedScope`), the source commit (`--source-commit`) and the manifest tree
  (`RuntimeOptions.PlanGit`, with no manifests) are injected, so the controller no longer starts about 1,700 `git`
  processes per seed. `TestHarness` fails if the process waited for any child during its seeds (the seeds fan out
  inside one group of a serial test, so nothing else runs beside them). A seed takes about 3 s instead of 25 s.
- **Saved programs misbehave.** Each detector has a stand-in program in the host whose Diagnose and Mitigate the seed
  makes right, wrong (opposite verdict, or a write to another object), garbage (invalid verdict or plan), failing,
  panicking or hanging, for the whole run. The controller's automatic response stage runs over them through the
  harness agent (`DeterministicResponseStage`), with the memory host serving the registry, the controller's snapshot
  provider and a plan executor writing through the faulty API. Program revisions and the manifest tree are
  served from memory (`ResponderLoop.GitRead`). The first run found two controller bugs: the remote program registry
  dropped scalar parameter kinds and defaults (a program with a scalar parameter could never bind to a finding), and a
  due dispatch retry held by another condition made `NextWake` return the due time forever (seed 10: the loop
  spun on a frozen clock).
- **A due wake no pass consumes.** The run loop fails with `ErrWakeSpin` after 1000 timer passes with no delay inside one second, naming the `NextWake` item (`nextWake` returns its label). A gate probe of a detector that a memory revision removed was never evaluated and never dropped, so it was due forever while an incident stayed open (seed 58 in the parallel sweep); `settleGateProbes` ends every probe that came due in a pass and was not rescheduled. `TestStepConsumesGateProbeOfRemovedDetector` pins it.
- **Informer sync never gives up.** At startup and on resume the controller retries the informer sync with backoff (`InformerSyncPolicy`: per-try timeout, 5 s doubling to 30 s) until it syncs or the run ends. It used to exit after three tries, which ended the process on a transient list error and, repeated, left the Job Failed. Seeds 8 and 48 fail without the fix.
- `--state-baseline=false` panics (a nil `StateTracker` in the diff read); the harness keeps the baseline on.
- Secrets are not observed (no `MetadataClient`).

## Not yet modeled

These are open gaps the simulation harness work closes; until then, cover them with integration-tagged
tests or kind runs.

- the agent tool transport, the Job executor and the closure, reflection and acknowledgement path
  (`NativeIncidentBroker`) under the harness (see its limits);
- operating-system memory-host process launch, resource limits and activation swap;
- NetworkPolicy enforcement and DNS.

The harness is a serial edge/event model, not a complete Kubernetes implementation. Pod transitions
and deployment completion are explicitly scheduled; it does not automatically spawn ReplicaSets,
schedule pods, pull images or run workloads. Kubelet measurements/log bytes are scenario inputs,
not a resource-consumption model. The cache models missed updates/relist, not client-go watch streams.
The HTTP transport currently supports snapshot lists, kubelet proxy and log reads; plan mutations use
`Kube` directly. Protocol peers implement the credential-free link probe handshakes, not databases or
service application logic. No kind conformance run was performed for this extraction; claims above
are bounded by the synthetic conformance tests.

## Synthetic Go toolchain

`internal/testutil.FakeGo` (operator module) stands in for the compiler edge. It models stage
selection (verb, package patterns covering a directory, and a test stage that selects no test
through `-run '^$'`) and exit status with canned output.
It does not model type checking, vet analyzers, test execution, the Go build cache, link
output, or replay results written by generated tests. The compile rows of the gate table
(`memory/validate/gate_table_test.go`) run on FakeGo and on the real compiler and must agree on
the rule and the path (`TestGateCompileRowsOnFakeGo`, `TestGateCompileRowsOnRealGo`); anything
beyond that has no test.

## Accepted bounds

HTTP connection pooling after an OS listener closes is not represented by the
handler-backed HTTP transport or credential-free pipe peers. The one real-socket
check of the prober is the integration-tagged `TestLinkProtocolsSocketReset`
(reset classification for every protocol over real TCP) and
`TestLinkGRPCTLSVerifiesCertificates` in `prober/link_socket_integration_test.go`;
pool reuse is no longer tested. PR and nightly Go CI run them with
`-tags=integration`. No NetworkPolicy enforcement claim is implied.

## Gap ledger

Rows dated before 2026-11-03 name regression tests that were later deleted with the legacy Go tests. They record how each gap was found and closed; the tests named there no longer guard it. New rows name a harness seed, conformance scenario or gate-table row.

| Date | Source run | Class | Gap | Status | Simulation change and conformance test |
| --- | --- | --- | --- | --- | --- |

| 2026-10-31 | Synthetic shared-world conformance | Missing behavior / timing / fault | Shared pod, input and protocol edges | Modeled within bounds above | `TestCrashLoopRecovery`, `TestFaultSchedule` (the real-detection-input and protocol-dialer tests were deleted in the 2026-11-03 Go test reduction) |

| 2026-10-31 | Synthetic memory-host slice 1 | Transport / evidence | Detector and program execution outside the controller | Modeled at RPC boundary | Same world, bufconn, host-owned log reads and typed receipt replay. Regression test deleted in the 2026-11-03 Go test reduction; not recreated |
| 2026-11-02 | Runtime test reduction | Missing behavior / cost | Broker compile gate repeated real Go builds in conflict and no-change tests | Modeled within bounds above | `sim.Compiler` stands in for the Go compiler where a test needs a compile gate without real builds; its recording test and the controller tests that used it were deleted in the 2026-11-03 Go test reduction. Real compilation stays in the builder and validator tests |
| 2026-11-03 | Kubernetes conformance (envtest) | Contract drift | The simulated API server diverged from a real one on defaults, server-side apply, patch, status, generation, delete and list/watch | Closed; bounds listed under Conformance bounds | `TestKubeConformance` scenarios, one per area |
| 2026-11-03 | Controller harness on the client-go fake | Missing behavior | The simulated API could not serve the controller loop: no Leases or RBAC kinds, store not safe for concurrent clients, no watch lag, a stuck watcher blocked writers | Closed in the simulation | `lease/*`, `rbac/*`, `watch/informer-list-then-watch`, `TestTransportServesConcurrentClients` |
| 2026-11-01 | Prober determinism migration | Transport | HTTP pool reuse after listener closure | Accepted bound | In-memory handlers intentionally omit TCP pooling; the pool tests were deleted, `TestLinkProtocolsSocketReset` keeps the real-socket reset check |

The harness's memory host runs over bufconn: it does not model OS process isolation, Unix socket
permissions, memory limits or actual compiler latency. The end-to-end run launches the real host
subprocess but does not test its limits.

Installed controllers have no accepted-memory imports or restart-on-memory path.
Embedded tests may inject synthetic SDK implementations and traffic adapters;
those seams do not compile accepted repository memory into the controller.
Bounded responder verification uses incident tool transport and the host Traffic
RPC; no agent-to-host connection or prober pod is modeled or deployed.

### Kind memory-host proof (2026-11-01)

The scripted Tier 2a run at `/mnt/data/shli/memory-host-toy-proof/`
uses a private kind lane, Calico and generated responder/reflection scripts,
without model calls. Its initial run exposed a missing fault case: a routing
probe against the broken target exhausted its four-second caller deadline;
`Manager.Traffic` treated that normal cancellation as a host crash, retired the
accepted process, and made concurrent detector/responder calls unavailable.
The smaller deterministic boundary can represent this behavior; it lacked a
regression test; it was deleted in the 2026-11-03 Go test reduction and not recreated.
The fix separates the caller deadline from the manager's own failure budget;
transport disconnects and manager timeouts retain crash recovery.

| Date | Source run | Class | Gap | Status | Simulation change and conformance test |
| --- | --- | --- | --- | --- | --- |
| 2026-11-01 | Tier 2a `20261101T021321Z-sdo` | Missing fault coverage | Caller probe deadline retired healthy accepted host | Fixed at manager boundary | Real routing timeout ordering minimized into a synthetic virtual-time fixture. Regression test deleted in the 2026-11-03 Go test reduction; not recreated |

The fixed real-kind run accepted reflection commit
`e2fb8cd7527215c73712210cfec6cec52c4b04b8` with binary digest
`sha256:774a6ea5a7a669f70b7cd27332d69f7b5850359c07742da94032838615214075`.
Build, catch-up and switch took **5,561 ms**, compared with the simulation's
injected 50 ms. The maximum interval between evaluation log records within ten
seconds of activation was **221 ms** (configured cadence about 200 ms), with
contiguous iteration numbers. The native ledger verified activation at
02:19:53.402907231Z and acknowledged at 02:19:53.433047666Z. A later standalone
backend incident fired the newly accepted `drift.service-targetport-mismatch`
detector and passed its repair oracle, with zero model tokens and no harness errors.

The matched empty-snapshot benchmark measured **0.25–0.32 ms** over bufconn,
**0.40–0.42 ms** through a separate Unix process on the host, and **0.86–1.24 ms**
through a separate Unix process in the controller Pod. Each used the same
ready-endpoints detector; the Unix helper ran 5,000 calls per trial, three trials.
These isolate mirror/adapter/transport overhead and exclude Kubernetes reads,
process startup and compilation. The earlier approximately 0.57 ms measurement
is not a bound on real Pod latency. OS scheduling and compiler cost remain
fidelity bounds covered by this kind run, not virtual-time latency predictions.

The documented 0.1 time scale also shortened the first run's harness verification
budget below cold worker/validator cost: `observe-controller` received a 1 ms
remaining deadline, although reflection later accepted and activated successfully.
This is a timing-model alignment limit: simulated turns do not include real image,
init-container and compiler delays. Keep controller cadence unchanged when
comparing pod continuity and give the scripted harness sufficient wall-time budget
(`--timeout 1800` here). Failed attempts remain alongside successful evidence;
their result rows are not counted as passing incidents.

| Date | Source run | Class | Gap | Status | Simulation change and conformance test |
| --- | --- | --- | --- | --- | --- |
| 2026-11-01 | Tier 2a memory-host proof, same external evidence root | Fidelity bound | Actual compiler/process/Unix transport costs exceed injected build delay and bufconn timing | Measured; not a simulation latency guarantee | Real kind activation log, contiguous evaluation trace, native ACK ledger and in-Pod Unix benchmark; no OS timing model added |
| 2026-11-01 | Tier 2a `20261101T021638Z-sdo` | Timing model | Scaling all harness deadlines does not scale cold init/compiler work | Accepted run-budget bound | Kind rerun with 1800-second harness timeout at unchanged 0.1 controller cadence; synthetic turn times remain explicit inputs |

The clean two-incident rerun (`20261101T022237Z-sdo`) passed both oracles and
strict receipts with zero errors and zero model tokens. Incident walls were
150.23/93.38 s; injection-to-mitigation was 39.83/34.83 s. The second repair used
`service-targetport-mismatch` with a hint binding. Pod UID
`a8dd5f26-a728-4b3f-bd04-632277475e85`, start `2026-11-01T02:22:21Z`, container
start `02:22:23Z`, and restart count zero were identical before and after.
Reflection commit `517b34983e852027fa3c28bee60fe17743ad8d5d` activated in
4,486 ms; the later outcome revision activated in 4,675 ms, with the same binary
digest above. First activation verification at `02:25:02.207702310Z` preceded
ACK at `02:25:02.234315225Z`.

Fastloop deliberately pauses observation during baseline recovery before draining
reflection. Consequently the clean stream's swaps prove pod continuity and ACK
ordering, **not uninterrupted evaluation**. The separate earlier fixed run above
provides that trace (100 contiguous evaluations within ten seconds on either side
of activation, maximum gap 221 ms). Its harness deadline failure remains explicit.
This distinction prevents interpreting planned maintenance as an activation stall.
The evidence root includes raw logs, before/after Pod JSON, native ledgers,
telemetry, benchmark source and `summary.json`; no run artifacts belong in Git.

| Date | Source run | Class | Gap | Status | Simulation change and conformance test |
| --- | --- | --- | --- | --- | --- |
| 2026-11-01 | Tier 2a `20261101T022237Z-sdo` | Timing / harness ordering | Fastloop recovery pauses detection before reflection drain | Observed intentional maintenance; clean stream cannot prove uninterrupted swaps | Separate active-detection trace above; do not infer no-gap behavior from pod continuity alone |

The final clean-run in-Pod benchmark repeated at **0.590–0.614 ms/evaluation**
(three trials of 5,000 calls); its raw records are `unix-pod-bench.jsonl`.
The earlier 0.86–1.24 ms trials above were taken amid the failed-run cleanup.
Both measurements demonstrate scheduling-dependent cost; the final repeat is
close to, but above, the historical approximately 0.57 ms figure.

### Remaining host adapter and CPU bounds (2026-11-01 determinism migration)

The state-baseline and restart-storm correctness cases use synctest virtual time,
including informer propagation, rate limiting and state-write counts. State
tracker tests opt into a one-hour informer resync so client-go's disabled-resync
channel, allocated outside the bubble, cannot pin virtual time. Production keeps
resync disabled. Negative resync intervals are rejected at Start.


### Protocol wire conformance boundary (2026-11-01)

Link protocol handshakes and malformed/authentication/status replies now run over
buffered in-memory connections inside synctest, including context deadlines and
a fixed one-millisecond establishment delay for positive duration evidence.
This preserves actual HTTP/2, Redis, MongoDB, PostgreSQL, MySQL and memcached wire
parsers rather than substituting precomputed probe results.

Kernel TCP reset delivery (`SetLinger(0)`) is outside that transport's model;
`TestLinkProtocolsSocketReset` retains the reset classification for every
application protocol against real TCP sockets. Default TLS certificate rejection
and the opt-in real Redis smoke also retain real sockets in
`prober/link_socket_integration_test.go`. These are accepted network-stack and
certificate-store bounds; `scripts/test_go_ci.sh` runs them with
`-tags=integration` (the Redis smoke still requires its endpoint environment).

Repeated race runs also exposed HTTP/2's unconfigured `ServeConn` fallback sharing
error channels across independent synctest bubbles. The fixture now calls
`http2.ConfigureServer` to obtain a per-server channel pool before `ServeConn`;
the full protocol matrix passes twenty race-enabled repetitions. This closes a
fixture ownership gap without changing the production protocol client.

### Merged-tree toy run on kind (2026-11-02)

The scripted Tier 2a run for the dev merge into main (`/mnt/data/shli/sdo-exp/port/merge-toy/`) found the
gaps below. Most were not reachable by the unit tiers because each test replaced the thing that changed
with a fixture that still described the old behavior.

| Date | Source run | Class | Gap | Status | Test that now covers it |
| --- | --- | --- | --- | --- | --- |
| 2026-11-02 | merge-toy | Missing fault coverage | Caller cancellation of `Manager.Evaluate` or a captured-host call retired the healthy accepted host; every detector reported `memory host unavailable` until recovery (about 15 s) | Fixed at the manager boundary | Regression test deleted in the 2026-11-03 Go test reduction; not recreated |
| 2026-11-02 | merge-toy | Contract drift | Memory-host registration dropped `ConsumesPodAdmission`; the controller never observed admission, the host's `pod-admission` detector errored every evaluation and the lane never reached a clear baseline | Fixed | Regression test deleted in the 2026-11-03 Go test reduction; not recreated (the fix keeps pod admission in the registration) |
| 2026-11-02 | merge-toy | Mock encodes removed behavior | Image smoke tests mocked `schema-identity` and `validation-child` arities that the binary no longer has, so every image build and `up` preflight failed on a real binary | Fixed | `test_image_build_smokes_offline_go_tools_as_nonroot` checks the child's arity; `test_fast_image_delivery.py` mocks `memory --help` |
| 2026-11-02 | merge-toy | Missing behavior | The stand-in `codex` never wrote a session rollout, which the production responder's session recovery needs at session start | Fixed | `test_scripted_session_persists_a_rollout_before_the_worker_checkpoints_it` |
| 2026-11-02 | merge-toy | Sandbox fidelity | The agent-script sandbox mounted the whole host read-only, but the real workspace-write sandbox lets `sdo` write `/tmp/sdo` and the session scratch | Fixed | `test_sandboxed_commands_can_persist_tool_replies_for_later_commands` |
| 2026-11-02 | merge-toy | Transport limit | A scripted file write put the whole file in one shell argument; a captured corpus exceeds the 128 KiB single-argument limit | Fixed (stdin) | `test_write_steps_larger_than_one_command_argument_succeed` |
| 2026-11-02 | merge-toy | Protocol drift | `sdo` prints a human summary unless `--json`; capped replies must be followed to `details_file` | Fixed | `test_tool_reads_json_and_follows_a_capped_reply_to_its_details_file` |
| 2026-11-02 | merge-toy | Protocol drift | `sdo verify` reports `awaiting_responder_completion` while the worker runs; polling for `healthy` for two minutes ended the scripted responder as failed and the outcome as failed | Fixed | `test_verify_loop_stops_when_detectors_clear_while_the_responder_still_runs` |
| 2026-11-02 | merge-toy | Lifecycle | A from-scratch toy install kept the previous controller checkpoint and health-refresh records; the checkpoint named a detector the reset memory did not have | Fixed | `test_retire_control_state_clears_checkpoints_and_stale_health_refresh_records` |
| 2026-11-02 | merge-toy | Lifecycle | A preloaded standby responder read its task before awaiting the claim and crashed; the controller recreated a standby Job every 9 s | Fixed | `test_generated_launch_waits_for_standby_claim_before_reading_the_task` |
| 2026-11-02 | merge-toy | Lifecycle | A health refresh ends the first controller attempt by design (exit 76), leaving a failed pod beside the running one; continuity required exactly one pod | Fixed | `test_live_controller_pod_ignores_pods_of_finished_job_attempts` |
| 2026-11-02 | merge-toy | Missing behavior | After a verified program write, a co-reported finding whose detector had not re-observed it (service-reachability on the same Service) made the attempt `applied_still_matching`; the stage gave up within a second and the responder finished a mitigated incident (3 of 4 repeats) | Fixed: findings that depend on the written object keep the recovery wait going and the attempt is recorded cleared | Regression test deleted in the 2026-11-03 Go test reduction; not recreated; existing stage tests unchanged |
| 2026-11-02 | merge-toy | Timing | Closure replays open-phase reads inside the observation lock; the program's open probe of the old targetPort waited out the 5 s probe budget (`program_reads_ms=5283`), stopping detection for 9.8 s | Bounded to 1 s (gap 9.8 s to 5.1-5.4 s); remaining closure work under the lock still exceeds the 5 s allowance by 0.15-0.4 s on the repeat | Regression test deleted in the 2026-11-03 Go test reduction; not recreated |
| 2026-11-02 | merge-closure | Timing | Closure captured the corpus, replayed reads and settled recovery applications inside the step pass, so no detector evaluated for 5.15–5.38 s around every closure; the simulation's closure work took zero virtual time and could not show it | Fixed (evidence gathers on a goroutine; the next pass freezes the closure) | Regression test deleted in the 2026-11-03 Go test reduction; not recreated |

Not modeled and still open: the Go simulation has no scripted-agent sandbox, standby handshake or Job
restart on exit 76, so the standby, sandbox, restart and verify-protocol rows are covered by Python contract tests only.
Closure work is modeled through the `closureEvidence` seam, which a test can make block; the closure recheck and the
incident-open capture still run on the step pass (about 0.6 s and 0.2 s on the toy lane) and are not simulated.

### Live toy run on Luna (2026-11-02)

The live run (`/mnt/data/shli/sdo-exp/port/merge-luna/`) never cleared a repeat in the deterministic stage. The
handoff blamed a frozen pre-apply snapshot; the stage record shows otherwise. The post-apply `Diagnose` observed
`targetPort 8080` (the repair had landed) and still matched, because the frozen source diff skipped the API
server's default normalization and read the defaulted `protocol: TCP` as drift added since acceptance. All verifications
take a fresh snapshot (`snapshotView`, the executor's health verifier); the stale-read sweep below found no reuse.

| Date | Source run | Class | Gap | Status | Test that now covers it |
| --- | --- | --- | --- | --- | --- |
| 2026-11-02 | merge-luna | Mock encodes removed behavior | Frozen diffs were computed on the live view with its API client removed, so the dry-run default normalization an in-process program gets was skipped: a repaired Service still matched, and the stage rolled back a correct repair | Fixed: diffs are the view's own answers, from the cycle cache, frozen before the host lease is taken | Regression test deleted in the 2026-11-03 Go test reduction; not recreated (it used the sim API with dry-run apply and Service defaults) |
| 2026-11-02 | merge-luna | Contract drift | The diff walk descended only wrappers it knew; a program mitigated through the isolation view (`mitigationReadView`) got no frozen diffs and its host reads found no evidence sink | Fixed: no type walk for diffs; every wrapper that embeds a `ReadView` exposes it | `TestReadViewWrappersExposeTheViewTheyWrap` (fails for any new wrapper that hides its view); the other tests were deleted in the 2026-11-03 Go test reduction |
| 2026-11-02 | merge-luna | Missing fault coverage | The scripted toy diagnosed from pod ports and planned from a pod, so no scripted program or detector ever called `DiffAgainstSource`; bugs above reached the first live run | Fixed: the scripted program, mitigation and incident detector are source-grounded like learned ones | `benchmarks/sregym/fastloop/scripted_program`, hermetic fastloop tests |

Reuse audit (any frozen observation used after the controller wrote): `matchedViews` is re-snapshotted once anything was applied
(`responder_loop.go`, `l.appliedAny()`); the stage's post-apply diagnosis and `verify` call `snapshotView`, and the plan verifier calls
`Provider.Snapshot`. `lastDiagnosisView` is read only for topology relations (selectors, labels), never for field values. Cost note: frozen diffs
now make one dry-run apply per object that has an accepted manifest per snapshot (cached across detectors and programs of that snapshot).

### Controller end-to-end: fake kubelet and limits

`scripts/test_controller_e2e.sh` runs the real controller process, memory-host subprocess and broker against a real
kube-apiserver and etcd (envtest) with a fake kubelet and the no-model agent scripts. What it models and what it does not:

- The API server is real; there are no nodes and no kube-controller-manager, so the fake kubelet supplies the Node, the
  Deployment, ReplicaSet, Endpoints, EndpointSlice and Job controllers, the default ServiceAccount, `kube-root-ca.crt` and PVC binding.
  Rollout is simplified (Recreate waits for old Pods; RollingUpdate waits for the new Pod to be ready). There is no scheduler,
  admission-plugin set beyond the API server's defaults, garbage collector beyond ReplicaSet and Job children, or TTL controller.
- There are no images. Every container runs on the host's userland in a bubblewrap sandbox with the Pod's volume mounts; one
  profile (`e2e/images_test.go`) supplies `sdo-operator`, the Go toolchain, the repository and the Python environment at the paths the
  production images use, and a shared warm Go build cache replaces the image's cache seed. File ownership, uid 65532, read-only
  root filesystems, resource limits, seccomp and capability drops are not enforced. A Pod's own env wins over image defaults except
  for a few forced host adaptations (`GOCACHE`, `GOMAXPROCS`, `GOTOOLCHAIN`). Init containers named `prepare-toolchain` are skipped.
- ConfigMap and Secret volumes are copied once when the Pod starts; a later update is not propagated. Probes are not run: a sidecar
  counts as started when its process starts.
- Application Pods are not executed. Their phase and readiness follow a small model of the toy application. Pods carry loopback
  `status.podIP` (127.1.x.y) with real listeners, because agents dial Pod IPs directly; the API server rejects loopback addresses
  in Endpoints, so Endpoints and EndpointSlices carry the 10.244.x.y twin and `targetRef` names the Pod. Anything that joins Endpoints to
  Pods by IP instead of `targetRef` would see a mismatch.
- There is no real network. Cluster DNS is a hosts file; each Service has a loopback forwarder that resets the connection when no
  ready Pod listens on the target port. NetworkPolicy is not enforced. The routing prober needs a "unicast Pod IP" and reports
  `routing_probe=unavailable` instead.
- The kubelet's usage endpoints (`stats/summary`, `metrics/cadvisor`) return fixed plausible values, not measurements. Node proxying
  needs a non-loopback host address (the API server refuses loopback), so the run requires one network interface with an IPv4 address.
- API faults come from a TLS reverse proxy in front of the API server that only the controller uses (error rate, latency, blackout). Workers
  and the harness go direct, so a crashing or hanging agent is injected by the wrapper command, not by API failure.
- Time is real. Cold start is dominated by the controller's own compile (about 100 s with a warm Go cache), and a full run takes several minutes.

Gaps this run found are recorded in the ledger below.

| Date | Source run | Class | Gap | Status | Test that covers it |
| --- | --- | --- | --- | --- | --- |
| 2026-11-03 | controller e2e | Missing behavior | The first run had no ServiceAccount controller: `cluster-wiring` reported `ServiceAccount/default` missing for every Deployment, so no repair ever verified | Fixed in the fake kubelet (default ServiceAccount and `kube-root-ca.crt` per namespace) | every `scenario1` run (an unverifiable repair times out) |
| 2026-11-03 | controller e2e | Missing behavior | The API server rejects loopback addresses in Endpoints; agents dial Pod IPs directly, so Pods carry loopback IPs and Endpoints carry a twin | Modeled within the bounds above | `scenario1` (the agent's drift assessment dials the Pod IP) |
| 2026-11-03 | controller e2e | Missing behavior | The API server will not proxy node requests to a loopback address, so `resource-saturation` failed `address not allowed` | Fixed: the Node carries a routable host address | every run (`diagnose` fails without it) |
| 2026-11-03 | ablation build envtest | Fault isolation | Killing a memory host scanned global `/proc` and could signal another World's or lane's host | Fixed: traverse only descendants of live controller sandboxes owned by the current Runtime | `TestMemoryHostFaultIsConfinedToOwnedSandbox` (also in the cheap hermetic gate) |
| 2026-11-03 | ablation build envtest | Missing behavior | The scripted launcher replaced the incident's persistent provider home with a temporary login, discarding transcripts required by controller continuations | Fixed: preserve an explicitly supplied provider home and existing credentials; retain a temporary fallback for standalone scripts | `test_script_launcher_keeps_the_incident_provider_home_across_worker_jobs` |
| 2026-11-03 | controller e2e | Controller finding | The controller exits on one transient API 500 (startup reads, lease renewal, steady-state listings); the controller Job gives up after 7 failures | Fixed: transient API errors are retried with backoff; `TestControllerToleratesAPIErrors` and harness check `ControllerExited` | `bash scripts/test_controller_e2e.sh api-errors` |
| 2026-11-03 | controller e2e | Controller finding | A hung responder Job is never ended: no `activeDeadlineSeconds` on incident Jobs, the response deadline is honoured by the worker only | Fixed: Job deadline plus a controller-owned end at the response deadline; `TestControllerEndsHungAgent`, and the harness agent stand-in no longer bounds itself | `bash scripts/test_controller_e2e.sh hung-agent` |
