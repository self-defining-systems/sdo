# Simulation fidelity

What the Go deterministic simulation models, what it deliberately does not, and the ledger of alignment
gaps between it and real Kubernetes. The [align-simulation skill](../.agents/skills/align-simulation/SKILL.md)
describes how gaps are found and closed; every closed or accepted gap updates this file.

## Modeled

The simulated API server (`sdo/operator/runtime/sim_kube_test.go`) models:

- server-side defaulting, quantity normalization and resourceVersion preconditions on patches;
- status-only resourceVersion churn between a caller's read and its patch;
- a patch that commits and then returns a transport error (outcome unknown to the caller);
- informer caches that keep serving the pre-write object for a seeded number of reads;
- a virtual clock whose sleeps advance time without blocking;
- one seeded RNG per run, consumed in call order, so a failing seed replays (`SDO_SIM_SEED`).

The pure lifecycle core (`runtime/lifecyclecore`) is covered by exhaustive explorers of closure and
reflection under bounded API faults, restarts and worker attempts (see `docs/lifecycle-state-machine.md`).

## Not yet modeled

These are open gaps the simulation harness work closes; until then, cover them with integration-tagged
tests or kind runs.

- the real controller driver end to end (deferred slices of the lifecycle plan);
- pod lifecycle and rollout progression timing; readiness lag;
- kubelet resource usage, container logs and terminated-container tails;
- service-to-service protocol endpoints and network partitions;
- the memory-host subprocess, its swap, crash and restart;
- NetworkPolicy enforcement and DNS.

## Accepted bounds

None recorded yet. Each entry names the behavior, why it is not modeled, and the integration-tagged test
that covers it.

## Gap ledger

| Date | Source run | Class | Gap | Status | Simulation change and conformance test |
| --- | --- | --- | --- | --- | --- |
