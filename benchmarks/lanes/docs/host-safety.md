# Host safety gates

A runaway lane must never be able to take a host down, and the platform must stop adding load when a host shows the
early signs. The rules for every gate here:

- A gate only **stops new work** and **raises an alarm**. It never kills, tears down or resets anything.
- One clear signal per gate, a bound (timeout) on every probe, and a reason string shown in `lanes host status` and
  `lanes plan status`.
- A probe that fails or times out counts as unhealthy. This fails closed for admission only.
- Thresholds live in `hosts.yaml` with defaults, so a host can be tuned without a code change.

## 1. Memory is reserved, and the limit is real

Each lane declares its memory (`lane.memory` of the spec, default 12Gi for kind-only apps and 30Gi for larger ones).

- **Admission.** The lane's step takes its cores and memory from the host ledger (`CPUAllocator.Take`). The sum of the
  admitted lanes' memory may not exceed the host budget: `reserve.memory`, else `lanes.memory` of `hosts.yaml`, else
  85 % of the host's RAM. A lane that does not fit waits (`ErrNoFreeMemory`); one that can never fit fails as a
  configuration error.
- **Hard limit.** The amount is applied to the node containers as `docker run --memory/--memory-swap` (through
  `deploy/lane-docker-shim`) and re-applied with `docker update` (`deploy/lane-limits.sh`). The shares of the lane's
  nodes add up to the lane's total; swap equals memory, so there is none.
- **Read back.** After a lane is up, `deploy/kind-lane.sh` reads `HostConfig.Memory` of every node (the load balancer
  excepted) and fails the up, deleting the lane just made, if one reads 0. A limit that was only handed to docker is not
  counted as a limit.

Tests: `TestAllocatorMemoryBudget`, `TestMemorySharesAddUpToTheLaneTotal`, `TestShimAddsLimitsToKindNodes`,
`TestKindLaneUpChecksTheMemoryLimitIsApplied`.

## Churn budget

Every cluster create, delete or reset takes a network namespace under the kernel's global lock. `max_builds` of a host is
the budget for all of them together, not only for builds: a build, a release (which resets the lane to its golden copy
or deletes its cluster), an incident reset and a recover each count as one churn step. A new build, release or reset
waits while the host runs `max_builds` of them (`lanes status` shows the count as `builds n/N`). A recover, the cleanup
of a step that died, counts but is never held back: what a dead step left must be undone. Test:
`TestChurnBudgetCoversReleasesAndResets`.

## Crash-loop detector

Pods killed at their memory limit restart again and again, and every restart creates a container and a network
namespace. Once per tick the manager makes one bounded read per lane with a cluster (`kubectl get pods -A -o json`,
15s, over the host's transport). A lane whose containers restarted more than `crashloop.restarts` times (default 10)
within `crashloop.window` (default 10m) is crash-looping: it gets no new build or run (the reason is its idle reason in
`lanes status`) and the alarm `lane-crash-looping` fires once. Nothing is killed; release, recover and reset still run.
Restarts are summed between consecutive readings, so history before the first reading, a deleted and recreated pod and
a pod that merely exists with old restarts do not count. A failed read is no reading: it neither marks nor clears the
lane. The lane is released as the restarts age out of the window. Tests: `TestCrashTrackerCountsRestartsInTheWindowOnly`,
`TestCrashLoopingLaneGetsNoNewSteps`.

```yaml
hosts:
  spokane1:
    crashloop: {restarts: 10, window: 10m}
```
