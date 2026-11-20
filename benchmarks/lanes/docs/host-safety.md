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

## 2. Host health gate

Creating or destroying a container takes a network namespace under the kernel's global rtnl lock. When the lock jams,
dockerd, kubelet, containerd and sshd hang with it and the host is lost until a reboot. The gate watches the earliest
signs, once per manager tick and per host.

- **Signal.** One bounded probe over the host's existing transport (ssh, or a local `sh`): the time of `ip -br link`
  (it takes the same lock; `timeout 8`) and the kernel's hung-task count (`/proc/sys/kernel/hung_task_detect_count`,
  else the number of "blocked for more than" lines in `dmesg`; unreadable means no signal, not unhealthy). The whole
  probe is bounded by `health.probe_timeout` (default 10s). Over ssh the probe goes first, and a host that fails it
  is asked nothing else, so a jammed host costs one probe timeout.
- **Unhealthy** when the health probe fails or times out (a host that does not answer at all is the manager's separate
  "unreachable" state, with its own recovery), when `ip -br link` takes longer than `health.ip_slow` (default 2s) in
  `health.slow_probes` probes in a row (default 2), or when the hung-task count rises.
- **Healthy again** after `health.good_probes` (default 3) consecutive good probes. A slow or bad probe restarts the count.
- **Effect.** The host takes no new lanes or steps: no build, run or release is started on it, and every lane waiting
  for it says why (`host h is unhealthy: ...`). Steps already running are left alone, finished steps are still adopted
  and a dead step still gets its recover step. Nothing is killed, torn down or reset.
- **Alarm.** `host-unhealthy` in `alarms.jsonl` (shown by `lanes status`) once when the host turns unhealthy, and a line
  in `bench.log` when it recovers. `lanes status` shows the reason on the host's line.
- **State** is in the manager's memory. A restarted manager starts healthy with no hung-task baseline; the first bad
  probe stops the host again.

`hosts.yaml`, all optional:

```yaml
hosts:
  spokane1:
    health: {probe_timeout: 10s, ip_slow: 2s, slow_probes: 2, good_probes: 3}
```

Tests: `TestHostHealthStateMachine`, `TestHostHealthThresholdsComeFromTheInventory`,
`TestUnhealthyHostStopsNewWorkAndAlarms`, and with fake `ip`/ssh `TestLocalCanary*`, `TestSSHProbe*`.

### Memory pressure

The same probe reads `/proc/pressure/memory`. While the "full" avg60 is over `health.mem_psi_full` (percent, default 40)
the host takes no new work (reason `memory pressure (PSI full avg60) is 65%, over 40%`, alarm `memory-pressure` once per
rise). Memory-capped clusters thrashing, ours or another team's, are felt by every lane on the host. avg60 is already a
minute's average, so there is no hysteresis. Test: `TestMemoryPressureStopsAdmission`.

## 3. Kernel minimums

Kind clusters need many inotify instances and memory maps. `hosts.yaml` may set `kernel_minimums` (defaults:
`fs.inotify.max_user_instances` 8192, `fs.inotify.max_user_watches` 1048576, `vm.max_map_count` 262144; a host adds or
overrides keys).

- `lanes host adopt|reserve` asks the root helper (`kernel raise KEY=MIN...`) to write `/etc/sysctl.d/99-zz-lanes.conf`
  and apply it. Values are only raised: a live value above its minimum is not touched, and the file keeps the highest
  value it has held.
- Admission reads the **effective** values (`sysctl -n`) in the per-tick probe. A later file can win (`/etc/sysctl.conf`
  is applied last), so below a minimum the host is not admitted and the reason names the setting and the file and line
  that sets it. `lanes host status` prints the same. The alarm `kernel-setting-low` fires when a value is found low,
  also when it drops while lanes run. Nothing is changed automatically after that.
- A shared RDMA network namespace (`rdma system show` says `netns shared`) with `ib_core` loaded is a host warning in
  `lanes status` and `lanes host status`. It is never a gate and never changed automatically.

Tests: `TestKernelRaiseOnlyRaises`, `TestHostAdoptAndReserveRaiseTheKernelMinimums`,
`TestKernelSettingBelowMinimumStopsAdmissionAndAlarms`, `TestLocalCanaryReadsKernelSettingsAndSaysWhereTheyAreSet`.
