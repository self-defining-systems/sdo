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
