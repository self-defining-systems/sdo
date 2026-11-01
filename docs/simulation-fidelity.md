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

None recorded yet. Each entry names the behavior, why it is not modeled, and the integration-tagged test
that covers it.

## Gap ledger

| Date | Source run | Class | Gap | Status | Simulation change and conformance test |
| --- | --- | --- | --- | --- | --- |

| 2026-10-31 | Synthetic shared-world conformance | Missing behavior / timing / fault | Shared pod, input and protocol edges | Modeled within bounds above | `TestCrashLoopRecovery`, `TestSimRealDetectionInputs`, `TestFaultSchedule`, `TestHermeticProtocolDialer` |

| 2026-10-31 | Synthetic memory-host slice 1 | Transport / evidence | Detector and program execution outside the controller | Modeled at RPC boundary | `TestSimMemoryHostParityAndCrash`, `TestRemoteMemoryPreservesInputsAndCorpusReceipts`; same world, bufconn, host-owned log reads and typed receipt replay |

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
cost. No live Kubernetes hot-activation experiment has been performed.

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
