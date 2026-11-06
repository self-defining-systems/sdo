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

| Date | Source kind | Class | Gap | Status | Simulation change and conformance test |
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

### Memory-host process and timing bounds

Caller cancellation must not retire a healthy accepted host. The manager separates
caller deadlines from its own failure budget; disconnects and manager timeouts
retain crash recovery. Compiler, process and Unix transport costs remain outside
the virtual-time model. Harness deadlines must allow cold initialization without
changing controller cadence. Fastloop pauses detection during baseline recovery
before draining reflection, so pod continuity does not establish uninterrupted evaluation.

| Date | Source kind | Class | Gap | Status | Simulation change and conformance test |
| --- | --- | --- | --- | --- | --- |
| 2026-11-01 | kind scripted run | Missing fault coverage | Caller probe deadline retired healthy accepted host | Fixed at manager boundary | Real routing timeout ordering minimized into a synthetic virtual-time fixture. Regression test deleted in the 2026-11-03 Go test reduction; not recreated |
| 2026-11-01 | kind scripted run | Fidelity bound | Actual compiler/process/Unix transport costs exceed injected build delay and bufconn timing | Accepted OS timing bound | No OS timing model; process and compiler latency require kind validation |
| 2026-11-01 | kind scripted run | Timing model | Scaling all harness deadlines does not scale cold init/compiler work | Accepted run-budget bound | Harness deadlines must cover cold initialization; synthetic turn times remain explicit inputs |
| 2026-11-01 | kind scripted run | Timing / harness ordering | Fastloop recovery pauses detection before reflection drain | Intentional maintenance; pod continuity alone cannot prove uninterrupted evaluation | Require an active-detection trace independently of pod continuity |

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
the protocol matrix guards channel ownership across independent bubbles. This closes a
fixture ownership gap without changing the production protocol client.

### Scripted kind integration gaps

| Date | Source kind | Class | Gap | Status | Test that now covers it |
| --- | --- | --- | --- | --- | --- |
| 2026-11-02 | kind scripted run | Missing fault coverage | Caller cancellation of `Manager.Evaluate` or a captured-host call retired the healthy accepted host; every detector reported `memory host unavailable` until recovery | Fixed at the manager boundary | Regression test deleted in the 2026-11-03 Go test reduction; not recreated |
| 2026-11-02 | kind scripted run | Contract drift | Memory-host registration dropped `ConsumesPodAdmission`; the controller never observed admission, the host's `pod-admission` detector errored every evaluation and the lane never reached a clear baseline | Fixed | Regression test deleted in the 2026-11-03 Go test reduction; not recreated (the fix keeps pod admission in the registration) |
| 2026-11-04 | live SREGym run | Missing behavior | Independent verification (`--evaluate-once`) set pod admission (and the log client) by type-asserting the Kubernetes provider after the usage wrapper had hidden it, so accepted memory with the generic `pod-admission` detector failed verification; an injected provider got no admission at all, and a memory-host swap kept the informer cache's admission flag from the first bundle | Fixed: `detectorObservations.compose` derives pod admission, usage, logs and metrics from the detector declarations for the controller loop, each memory swap and `--evaluate-once`, and refuses a client-backed declaration without a client | `runtime/detector_observations_test.go` |
| 2026-11-02 | kind scripted run | Mock encodes removed behavior | Image smoke tests mocked `schema-identity` and `validation-child` arities that the binary no longer has, so every image build and `up` preflight failed on a real binary | Fixed | `test_image_build_smokes_offline_go_tools_as_nonroot` checks the child's arity; `test_fast_image_delivery.py` mocks `memory --help` |
| 2026-11-02 | kind scripted run | Missing behavior | The stand-in `codex` never wrote a session rollout, which the production responder's session recovery needs at session start | Fixed | `test_scripted_session_persists_a_rollout_before_the_worker_checkpoints_it` |
| 2026-11-02 | kind scripted run | Sandbox fidelity | The agent-script sandbox mounted the whole host read-only, but the real workspace-write sandbox lets `sdo` write `/tmp/sdo` and the session scratch | Fixed | `test_sandboxed_commands_can_persist_tool_replies_for_later_commands` |
| 2026-11-02 | kind scripted run | Transport limit | A scripted file write put the whole file in one shell argument; a captured corpus exceeds the 128 KiB single-argument limit | Fixed (stdin) | `test_write_steps_larger_than_one_command_argument_succeed` |
| 2026-11-02 | kind scripted run | Protocol drift | `sdo` prints a human summary unless `--json`; capped replies must be followed to `details_file` | Fixed | `test_tool_reads_json_and_follows_a_capped_reply_to_its_details_file` |
| 2026-11-02 | kind scripted run | Protocol drift | `sdo verify` reports `awaiting_responder_completion` while the worker runs; polling for `healthy` for two minutes ended the scripted responder as failed and the outcome as failed | Fixed | `test_verify_loop_stops_when_detectors_clear_while_the_responder_still_runs` |
| 2026-11-02 | kind scripted run | Lifecycle | A from-scratch toy install kept the previous controller checkpoint and health-refresh records; the checkpoint named a detector the reset memory did not have | Fixed | `test_retire_control_state_clears_checkpoints_and_stale_health_refresh_records` |
| 2026-11-02 | kind scripted run | Lifecycle | A preloaded standby responder read its task before awaiting the claim and crashed; the controller repeatedly recreated standby Jobs | Fixed | `test_generated_launch_waits_for_standby_claim_before_reading_the_task` |
| 2026-11-02 | kind scripted run | Lifecycle | A health refresh ends the first controller attempt by design (exit 76), leaving a failed pod beside the running one; continuity required exactly one pod | Fixed | `test_live_controller_pod_ignores_pods_of_finished_job_attempts` |
| 2026-11-02 | kind scripted run | Missing behavior | After a verified program write, a co-reported finding whose detector had not re-observed it (service-reachability on the same Service) made the attempt `applied_still_matching`; the stage gave up within a second and the responder finished a mitigated incident | Fixed: findings that depend on the written object keep the recovery wait going and the attempt is recorded cleared | Regression test deleted in the 2026-11-03 Go test reduction; not recreated; existing stage tests unchanged |
| 2026-11-04 | live run (P7 R3b) | Missing behavior | In-stage recovery waits exited `FAILED` about 10 s after the restarts, once the rollout finished, while windowed stall and traffic detectors still held pre-repair samples; an idle responder was dispatched and the deterministic fix was classified `cleared_without_action` | Fixed: an applied write keeps the wait open while it rolls out and then for its dependent findings' detector windows (incident findings included); persisting faults still hand off after the window | `TestRecoveryWaitSettlesWhileEffectsAndWindowsCatchUp` |
| 2026-11-04 | review (SWEEP2 #3) | Missing behavior | After #814 the recovery wait could end `CLEARED` (no responder) while the applied program still matched, or when a learned traffic detector went quiet because samples stopped arriving; the closure recheck had no window for learned traffic detectors, and a controller restart granted a fresh settling window | Fixed: `CLEARED` needs fresh post-write samples and no still-matching program, otherwise the cleared findings go to the responder; learned traffic consumers are confirmed by their workloads' samples and bound the closure recheck; the settling start is checkpointed | `TestRecoveryWaitSettlesWhileEffectsAndWindowsCatchUp`, `TestRecoveryWaitWindowSurvivesControllerRestart`, `TestTrafficRecoveryEvidenceNeedsFreshSamples` |
| 2026-11-02 | kind scripted run | Timing | Closure replays open-phase reads inside the observation lock; the program's open probe of the old targetPort exhausted its probe budget and stalled detection | Probe deadline bounded; remaining closure work under the lock is still a fidelity gap | Regression test deleted in the 2026-11-03 Go test reduction; not recreated |
| 2026-11-02 | kind scripted run | Timing | Closure captured the corpus, replayed reads and settled recovery applications inside the step pass, stalling detector evaluation during closure; the simulation's closure work took zero virtual time and could not show it | Fixed (evidence gathers on a goroutine; the next pass freezes the closure) | Regression test deleted in the 2026-11-03 Go test reduction; not recreated |

Not modeled and still open: the Go simulation has no scripted-agent sandbox, standby handshake or Job
restart on exit 76, so the standby, sandbox, restart and verify-protocol rows are covered by Python contract tests only.
Closure work is modeled through the `closureEvidence` seam, which a test can make block; the closure recheck and the
incident-open capture still run on the step pass and are not simulated.

### Live source-normalization gaps

| Date | Source kind | Class | Gap | Status | Test that now covers it |
| --- | --- | --- | --- | --- | --- |
| 2026-11-02 | live run | Mock encodes removed behavior | Frozen diffs were computed on the live view with its API client removed, so the dry-run default normalization an in-process program gets was skipped: a repaired Service still matched, and the stage rolled back a correct repair | Fixed: diffs are the view's own answers, from the cycle cache, frozen before the host lease is taken | Regression test deleted in the 2026-11-03 Go test reduction; not recreated (it used the sim API with dry-run apply and Service defaults) |
| 2026-11-02 | live run | Contract drift | The diff walk descended only wrappers it knew; a program mitigated through the isolation view (`mitigationReadView`) got no frozen diffs and its host reads found no evidence sink | Fixed: no type walk for diffs; every wrapper that embeds a `ReadView` exposes it | `TestReadViewWrappersExposeTheViewTheyWrap` (fails for any new wrapper that hides its view); the other tests were deleted in the 2026-11-03 Go test reduction |
| 2026-11-02 | live run | Missing fault coverage | The scripted toy diagnosed from pod ports and planned from a pod, so no scripted program or detector ever called `DiffAgainstSource`; bugs above reached the first live run | Fixed: the scripted program, mitigation and incident detector are source-grounded like learned ones | `benchmarks/sregym/fastloop/scripted_program`, hermetic fastloop tests |

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
  profile (`runtime/e2e/images_test.go`) supplies `sdo-operator`, the Go toolchain, the repository and the Python environment at the paths the
  production images use, and a shared warm Go build cache replaces the image's cache seed. File ownership, uid 65532, read-only
  root filesystems, resource limits, seccomp and capability drops are not enforced. A Pod's own env wins over image defaults except
  for a few forced host adaptations (`GOCACHE`, `GOMAXPROCS`, `GOTOOLCHAIN`). Init containers named `prepare-toolchain` are skipped.
- ConfigMap and Secret volumes are copied once when the Pod starts; a later update is not propagated. Probes are not run: a sidecar
  counts as started when its process starts.
- Application Pods are not executed. Their phase and readiness follow a small model of the toy application. Pods carry loopback
  `status.podIP` (127.<slot>.x.y, a range each end-to-end process claims so concurrent runs on one host never share an address) with real listeners, because agents dial Pod IPs directly; the API server rejects loopback addresses
  in Endpoints, so Endpoints and EndpointSlices carry the 10.244.x.y twin and `targetRef` names the Pod. Anything that joins Endpoints to
  Pods by IP instead of `targetRef` would see a mismatch.
- There is no real network. Cluster DNS is a hosts file; each Service exposes a loopback forwarder only while its selected upstream accepts TCP, so a missing
  ready endpoint or closed target port refuses the initial handshake. NetworkPolicy is not enforced. The routing prober needs a "unicast Pod IP" and reports
  `routing_probe=unavailable` instead.
- The kubelet's usage endpoints (`stats/summary`, `metrics/cadvisor`) return fixed plausible values, not measurements. Node proxying
  needs a non-loopback host address (the API server refuses loopback), so the run requires one network interface with an IPv4 address.
- API faults come from a TLS reverse proxy in front of the API server that only the controller uses (error rate, latency, blackout). Workers
  and the harness go direct, so a crashing or hanging agent is injected by the wrapper command, not by API failure.
- Time is real; cold compilation is not represented by virtual-time inputs.

Gaps this run found are recorded in the ledger below.

| Date | Source kind | Class | Gap | Status | Test that covers it |
| --- | --- | --- | --- | --- | --- |
| 2026-11-03 | envtest | Missing behavior | The first run had no ServiceAccount controller: `cluster-wiring` reported `ServiceAccount/default` missing for every Deployment, so no repair ever verified | Fixed in the fake kubelet (default ServiceAccount and `kube-root-ca.crt` per namespace) | every `scenario1` run (an unverifiable repair times out) |
| 2026-11-03 | envtest | Missing behavior | The API server rejects loopback addresses in Endpoints; agents dial Pod IPs directly, so Pods carry loopback IPs and Endpoints carry a twin | Modeled within the bounds above | `scenario1` (the agent's drift assessment dials the Pod IP) |
| 2026-11-03 | envtest | Missing behavior | The API server will not proxy node requests to a loopback address, so `resource-saturation` failed `address not allowed` | Fixed: the Node carries a routable host address | every run (`diagnose` fails without it) |
| 2026-11-03 | envtest | Fault isolation | Killing a memory host scanned global `/proc` and could signal another World's or lane's host | Fixed: traverse only descendants of live controller sandboxes owned by the current Runtime | `TestMemoryHostFaultIsConfinedToOwnedSandbox` (also in the cheap hermetic gate) |
| 2026-11-03 | envtest | Missing behavior | The scripted launcher replaced the incident's persistent provider home with a temporary login, discarding transcripts required by controller continuations | Fixed: preserve an explicitly supplied provider home and existing credentials; retain a temporary fallback for standalone scripts | `test_script_launcher_keeps_the_incident_provider_home_across_worker_jobs` |
| 2026-11-03 | envtest | Controller finding | The controller exits on one transient API 500 (startup reads, lease renewal, steady-state listings); the controller Job gives up after 7 failures | Fixed: transient API errors are retried with backoff; `TestControllerToleratesAPIErrors` and harness check `ControllerExited` | `bash scripts/test_controller_e2e.sh api-errors` |
| 2026-11-03 | envtest | Controller finding | A hung responder Job is never ended: no `activeDeadlineSeconds` on incident Jobs, the response deadline is honoured by the worker only | Fixed: Job deadline plus a controller-owned end at the response deadline; `TestControllerEndsHungAgent`, and the harness agent stand-in no longer bounds itself | `bash scripts/test_controller_e2e.sh hung-agent` |

| 2026-11-03 | envtest | Network fidelity | A Service forwarder accepted TCP before dialing its upstream, hiding every wrong targetPort from TCP health probes | Fixed: expose the virtual Service only while its upstream accepts TCP | `TestServiceTCPProbeTracksTargetPort` |
| 2026-11-03 | envtest | Missing behavior | No garbage collector released foreground deletion finalizers; deleting Jobs could keep scheduling worker Pods and prevent verified incident publication | Fixed: delete dependents, release only the GC-owned finalizer after they disappear, and never reconcile a deleting Job | `TestDeletingJobRetiresDependentsWithoutReplacement` |

### Memory preparation cost bounds

Canned diff methods in the old tool-speed benchmark did not perform production
snapshot object lookup. An individual lookup reconverted and sorted every typed
object; a full memory pass therefore projected a snapshot once per object.

The benchmark now uses the real `LiveReadView` for diffs. The cheap regressions
`TestLiveSnapshotObjectProjectionBudget` and `TestReadSnapshotDiscoveryBudget`
count projections and discovery requests per fresh cycle, independently of wall
time. The latter uses the real client-go HTTP transport and preserves one fresh
SSA normalization per object; API responses and errors are never cached across
snapshots. `memoryhost.TestObjectIndexSharesProjectionAndReturnsPrivateCapturedObjects`
checks concurrent reads, exact identities, private copies and captured-object
precedence. The seeded harness exercises the same production cycle views.
The simulation does not assign CPU/GC duration to reflection or serialization;
request/projection budgets cover that bound without adding machine-speed asserts.

Serialized dry-run API calls
held the cycle preparation mutex, blocking telemetry and unrelated detectors.
`TestFrozenPreparationOverlapsBoundedReads` uses a synctest barrier to require
exactly eight independent reads to overlap; existing receipt-budget tests still
require a single read per object and cycle. Host mirror versions now decode once
and clone typed Kubernetes objects directly for each RPC. The public read-factory
contract test `TestMirrorDecodingKeepsRPCCopiesAndHistoricalVersionsPrivate`
checks mutation isolation (including Jobs and Secret metadata) and retained
sequences; no admission result is reused between controller snapshots.

Reference enumeration and mirror capture must share wire projection; already
decoded traffic trees must avoid repeated protobuf JSON encoding on each RPC. `TestMirrorProjectionSharedByRefsAndCapture` requires
one projection per cycle across both consumers; `BenchmarkObservationDecode`
models nested traffic windows. Native-map bridging retains the protobuf JSON
failure semantics for unset values, non-finite numbers and invalid UTF-8, checked
by `TestObservationJSONMatchesProtobufJSONSemantics`.

Native broker initialization was mistaken for an externally
injected client, collapsing the existing state/lease isolation; responder reads
also shared the steady detector-normalization bucket. Production now owns
separate snapshot, state, leadership and incident-read budgets. The cheap
`TestNativeAndEmbeddedClientBudgets` injects the client factory: native roles must
be independent, while embedded simulations must retain exactly their supplied
API boundary. The per-client QPS/burst settings are unchanged.

The responder's status/detect loops also serialized detector execution, unlike
the controller's bounded batch, and detect bypassed the incident read factory.
`TestResponderToolDetectorBatchOverlaps` uses a synctest barrier to require eight
independent evaluations to enter before any may finish and one incident read
factory call. Both tools now use the controller batch evaluator; existing tool
scopes span detectors and program diagnosis so the mirror capture budget stays
one per fresh snapshot.
