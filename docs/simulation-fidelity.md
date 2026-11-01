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
- registered invariant hooks and a 45-second host-time seed-sweep budget.

The pure lifecycle core (`runtime/lifecyclecore`) is covered by exhaustive explorers of closure and
reflection under bounded API faults, restarts and worker attempts (see `docs/lifecycle-state-machine.md`).

## Not yet modeled

These are open gaps the simulation harness work closes; until then, cover them with integration-tagged
tests or kind runs.

- the real controller driver end to end (deferred slices of the lifecycle plan);
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

## Accepted bounds

HTTP connection pooling after an OS listener closes is not represented by the
handler-backed HTTP transport or credential-free pipe peers. Pool reuse and
fresh-dial refusal are transport implementation contracts, covered with real
sockets by the integration-tagged
`TestKeepAliveConnectionsWouldMaskABlockedListener` and
`TestBlockedListenerIsClassifiedAsADialFailureWithinAFewSeconds` in
`prober/socket_integration_test.go`. PR and nightly Go CI run both with
`-tags=integration`. No NetworkPolicy enforcement claim is implied.

## Gap ledger

| Date | Source run | Class | Gap | Status | Simulation change and conformance test |
| --- | --- | --- | --- | --- | --- |

| 2026-10-31 | Synthetic shared-world conformance | Missing behavior / timing / fault | Shared pod, input and protocol edges | Modeled within bounds above | `TestCrashLoopRecovery`, `TestSimRealDetectionInputs`, `TestFaultSchedule`, `TestHermeticProtocolDialer` |

| 2026-10-31 | Synthetic memory-host slice 1 | Transport / evidence | Detector and program execution outside the controller | Modeled at RPC boundary | `TestSimMemoryHostParityAndCrash`, `TestRemoteMemoryPreservesInputsAndCorpusReceipts`; same world, bufconn, host-owned log reads and typed receipt replay |
| 2026-11-01 | Prober determinism migration | Transport | HTTP pool reuse after listener closure | Accepted bound | In-memory handlers intentionally omit TCP pooling; `TestKeepAliveConnectionsWouldMaskABlockedListener` and `TestBlockedListenerIsClassifiedAsADialFailureWithinAFewSeconds` retain real-socket integration coverage |

The memory-host tests do not model OS process isolation, Unix socket permissions,
memory limits or actual compiler latency. Staged compilation is checked by
the integration-tagged `TestBuildMemoryHostUsesStagedMemory` in the nightly/local
integration tier. That test also launches a real host with a synthetic kubeconfig
and checks Ready identity; it does not exercise Kubernetes networking, OS limit
enforcement or a live activation swap.
`TestSimMemoryTwoAcceptedCommits` uses bufconn and injected accepted revisions;
`TestHostActivationGatesAcknowledgment` checks durable ledger activation and ACK
gating separately. These are boundary simulations, not a complete native broker
and controller driver run. The modeled build delay is 50 ms of virtual time;
it is not a production latency measurement. Runtime activation logs report actual
build-to-activation latency. `BenchmarkMemoryHostEvaluation` compares direct SDK
execution as a reference with mirror/adapter/bufconn overhead, excluding OS startup and compiler
cost. A scripted real-kind hot-activation check is recorded below; it is not an LLM learning experiment.

`TestSimMemoryTrafficTwoRevisions` compares the real traffic engine directly
against host-owned execution over bufconn in one virtual-time world. It covers
changed scenario catalogs, failed HTTP samples, scheduler stop/join, reset and
active-revision replacement. The HTTP application is an injected credential-free
Doer; it does not model cluster networking or DNS. `TestHostTrafficLifecycle`
checks duplicate start/stop, closed-host rejection and invalid primitive and
burst requests. These tests do not exercise the native controller driver or OS
scheduler/process isolation.

`TestTrafficActivationFailureAndCrashRecovery` injects scheduler-start rejection
and a traffic RPC disconnect at the manager boundary. Failed candidates preserve
the serving revision; crash recovery restores that revision and scheduler intent.
It does not model an OS crash. MemoryObservation no longer carries an inline
snapshot: transport parity tests use the same mirror capture barriers as production.

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
regression. `TestTrafficCallerDeadlinePreservesAcceptedHost` reproduces that
ordering under `synctest` and requires the same accepted host to remain available.
The fix separates the caller deadline from the manager's own failure budget;
transport disconnects and manager timeouts retain crash recovery.

| Date | Source run | Class | Gap | Status | Simulation change and conformance test |
| --- | --- | --- | --- | --- | --- |
| 2026-11-01 | Tier 2a `20261101T021321Z-sdo` | Missing fault coverage | Caller probe deadline retired healthy accepted host | Fixed at manager boundary | `TestTrafficCallerDeadlinePreservesAcceptedHost`; real routing timeout ordering minimized into synthetic virtual-time fixture |

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

Virtual time does not measure CPU consumed by `StateTracker.Changes`. The original
one-second host-process budget remains in `TestStateChangesStayFastAndBounded`
under `integration`; `TestStateChangesBounded` separately checks all truncation,
omission and removal invariants in virtual time. The optional restart-storm host
latency report also remains integration-only. These are accepted host-scheduler
bounds, not simulated duration measurements.

`state_baseline_secret_test.go` deliberately exercises the production kubeconfig
factory, real HTTP list/watch streaming and metadata-only decoder together. Its
socket test stays under `integration`; the canary, no typed Secret reads, strict
Accept headers, failed discovery and Secret identity assertions are unchanged.
The simulation does not model OS kubeconfig discovery or HTTP streaming framing.
The live kubelet logs and proxy-usage checks also require real cluster sockets
and retain their existing opt-in environment gates under `integration`.
`scripts/test_go_ci.sh` supplies `-tags=integration` for these tests; this move
changes no CI selections or opt-in prerequisites.
