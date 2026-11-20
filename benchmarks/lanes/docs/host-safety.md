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
- **State** (reason, probe counters, hung-task baseline) is kept in `<root>/hosthealth.json`, so a restarted manager
  neither forgets an unhealthy host nor takes hung tasks that appeared while it was down for old ones.

`hosts.yaml`, all optional:

```yaml
hosts:
  spokane1:
    health: {probe_timeout: 10s, ip_slow: 2s, slow_probes: 2, good_probes: 3}
```

Tests: `TestHostHealthStateMachine`, `TestHostHealthThresholdsComeFromTheInventory`,
`TestUnhealthyHostStopsNewWorkAndAlarms`, and with fake `ip`/ssh `TestLocalCanary*`, `TestSSHProbe*`.

### Memory pressure

Pressure is measured where our work runs. Host-wide memory pressure often sits inside another tenant's own memory limit
(a cluster thrashing at its memcg cap in `system.slice`) while our lanes are fine, so it must not refuse lanes.

- **Gate.** The same probe reads `lanes.slice/memory.pressure`, the whole lane block. While its "full" avg60 is over
  `health.mem_psi_full` (percent, default 40) the host takes no new work (reason `memory pressure of the lane block
  (lanes.slice PSI full avg60) is 65%, over 40%`, alarm `memory-pressure` once per rise). A host without `lanes.slice`
  has no reading and no gate. Per-lane pressure is the starvation gate below.
- **Alarm only.** Host-wide `/proc/pressure/memory` "full" avg60 over the same threshold raises `host-pressure` once per
  rise and never gates.
- The ip-link canary and hung tasks stay host-wide gates: they show a kernel-wide failure that hits every lane.

avg60 is already a minute's average, so there is no hysteresis. Test: `TestMemoryPressureGatesOnTheLaneBlockNotTheHost`.

### Lane starvation

The host-wide signals can look fine while one lane is starved: another tenant thrashes the cpus next to its cpuset, or
the lane hits its own memory cap. Probes then get too few samples and the run is invalid without anyone knowing. The
same probe call (no extra ssh) reads `lanes.slice/lanes-<lane>.slice/cpu.pressure` and `memory.pressure` (cgroup v2 PSI)
of every lane slice on the host.

- **Starved** when the lane's cpu "some" avg60 is over `health.lane_cpu_psi_some` (default 60 %) or its memory "full"
  avg60 is over `health.lane_mem_psi_full` (default 20 %). The reason is `unhealthy: starved (cpu|memory: ...)`, shown as
  the lane's idle reason. Defaults: a lane's own builds and the agent keep it busy, so cpu "some" of 30-50 % is normal
  load; above 60 % for a whole minute, runnable tasks wait most of the time. Memory "full" means every task of the cgroup
  was stalled on memory; a few percent is a lane near its cap, 20 % over a minute is thrashing, which is why it is
  stricter than the host's 40 %.
- **Effect.** The lane gets no new build or run (the same hold as a crash-looping lane). The step it runs is untouched;
  recover, release and unhold still run. A lane with no reading (no slice yet) is left as it is.
- **Alarm.** `lane-starved` once when the lane turns starved.
- **Recovery** after `health.good_probes` (default 3) good ticks in a row. The state is kept with the host health state in
  `<root>/hosthealth.json` (keys `lane:<lane>`).
- **Run record.** `run.json` `isolation` holds `psi_inject` and `psi_end` (`cpu_some`, `mem_full`): the lane's pressure
  when the fault was injected and when the run ended, so analysis can flag starved runs after the fact.

```yaml
hosts:
  spokane1:
    health: {lane_cpu_psi_some: 60, lane_mem_psi_full: 20}
```

Tests: `TestStarvedLaneGetsNoNewStepsAndRecoversAfterThreeGoodTicks`, `TestLanePSIThresholdsComeFromTheInventory`,
`TestRunRecordsLanePSIAtInjectAndEnd`, `TestLocalCanaryReadsLaneSlicePressure`.

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
