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
memory limits or accepted-commit activation. Staged compilation is checked by
the integration-tagged `TestBuildMemoryHostUsesStagedMemory` in PR CI. The first
slice sends full frozen snapshots per request; informer mirror fan-out and the
two-commit no-controller-restart scenario remain slice 2 work.
