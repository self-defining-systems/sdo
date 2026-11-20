# Lanes quickstart

How to run an eval without remembering commands. Every command takes `--root R` (or set `FUZZOPS_BENCH_ROOT=R` once).
For the full design see [plan.md](plan.md), [host-safety.md](host-safety.md) and [contract.md](contract.md).

## 1. What lanes is

`lanes serve` is a long-running manager. You give it a plan (arms x items x reps); it runs each experiment on an
isolated Kubernetes lane (a kind cluster with its own cores and memory) on your hosts, retries failures, and writes
results. It knows no benchmark; a benchmark plugs in as an adapter.

```bash
cd benchmarks/lanes && go build -o bin/lanes ./cmd/lanes
export FUZZOPS_BENCH_ROOT=/data/bench-root       # the results root: hosts.yaml, specs, plans, logs
bin/lanes manager start                          # start the manager as a systemd --user unit (restarts on failure)
bin/lanes manager status                         # unit state, binary path and revision, root
bin/lanes manager logs                           # follow the manager log (bench.log)
```

`lanes manager` runs `lanes serve` under a unit named from the root, so you never type `systemctl`. It checks that the
user has linger on (units die at logout without it) and prints the fix when not. It refuses to start while another
manager holds `<root>/bench.lock` and names that holder. `lanes serve` still runs the manager in the foreground, for
debugging. Every verb prints its own usage with `lanes VERB --help`.

## 2. Set up a host once

Add the host to `$FUZZOPS_BENCH_ROOT/hosts.yaml`. Keep `reserve`, `kernel_minimums` and `health` if you want to tune them;
the defaults suit most hosts (see host-safety.md).

```yaml
hosts:
  node1:
    ssh: node1.example               # or `local: true` for the manager's own machine
    staging: /var/tmp/lanes/staging
    kubeconfigs: /var/tmp/lanes/kube
    evidence: /var/tmp/lanes/evidence
    clusters: [c1, c2, c3]           # one lane each
    max_builds: 2                    # cluster creates, deletes and resets at once
    min_free: 150G                   # disk floor: no new build below it
    cluster_up: "$LANES_INSTALL/deploy/kind-lane.sh up {cluster} {kubeconfig} {evidence}"
    cluster_down: "$LANES_INSTALL/deploy/kind-lane.sh down {cluster} {kubeconfig}"
    reserve: {cores: 24, numa: 1, memory: 200Gi}     # the lane block; or `cpus: "8-31"`
    kernel_minimums: {vm.max_map_count: 262144}      # raised, never lowered
    health: {ip_slow: 2s, mem_psi_full: 40}          # thresholds of the host gates
```

Then install the root helper, apply the reservation, and look at the result.

```bash
lanes host install-helper node1     # puts the root helper and slice units on the host (asks for sudo)
lanes host reserve node1            # applies the reserve: block and the kernel minimums (or `reserve all`)
lanes host adopt node1 --cpus 8-31  # instead of reserve: record a block that already exists by hand
lanes host status node1             # check it
lanes host unreserve node1 --drain  # undo it, also while lanes run
```

`lanes host status` lines:

- `node1: block 8-31, two-way` is healthy: the block exists and every other slice stays off it. `one-way` means other
  work may still use those cores.
- `NOT ADMITTED, kernel: ...` names a kernel setting below its minimum and where it comes from. `reserve` raises it.
- `DRIFT: ...` means the slices no longer match the record: nothing is admitted until you fix or re-`reserve` it.
- `no record (lanes host adopt)` / `no block`: the host has no reservation; lanes run there only with `isolation: one-way-ok`.
- `helper unavailable (...)`: run `install-helper`.
- `DEFECT lane slice ...`, `our container ... outside the block`, `foreign container ... inside the block`: a lane or a
  container breaks the isolation; fix it before measuring.

`lanes status` shows hosts (`builds n/N`, load, free disk), lanes and the recent alarms.

## 3. Run an eval

A plan lists arms (each a spec template), items, groups of shared settings and edges that pass a verified artifact. This
one runs `p1` for each item, then `p2` seeded with what `p1` produced.

```yaml
plan: demo-1                         # at most 24 characters
pins: {workload: fuzzops@HEAD}       # refs are resolved once, so the run is reproducible
arms:
  - {name: p1, spec: specs/p1.yaml}
  - {name: p2, spec: specs/p2.yaml}  # its spec has operator.seed: "${artifact.memory}"
groups:
  shop: {vars: {pool: a}, spec: {namespace: shop}}
items:
  - {id: a, group: shop, vars: {fault: "a@x"}}
  - {id: b, group: shop, vars: {fault: "b@y"}}
reps: 2
parallel: 4                          # experiments released and not yet ended
artifacts: {memory: {path: "${run}/operator/learned-baseline", verify: exists}}
edges: [{from: p1, to: p2, artifact: memory}]
alarms: {factor: 2, idle_lane: 10m}
```

```bash
lanes plan add plan.yaml --dry-run   # runs every check and prints the nodes in run order; stores nothing
lanes plan add plan.yaml             # stores it; the manager starts releasing work on its next pass
lanes plan status demo-1             # per item and arm: done, in flight, queued, blocked, parked; alarms standing
lanes plan results demo-1 --tsv --final --done   # one row per finished node, tab-separated
lanes plan results demo-1 --tsv --grade-fields valid,score   # add columns from the grade (use your grade's fields)
```

`--dry-run` is where most mistakes show up: a bad spec, an unknown fault ID, or a host whose pinned checkout does not
report the pinned revision. Fix them there, not after the run starts.

## 4. Control

```bash
lanes plan pause demo-1              # release nothing new; runs in flight finish
lanes plan resume demo-1
lanes plan skip demo-1 b             # skip an item's unreleased nodes (add --cancel to stop its runs too)
lanes plan unskip demo-1 b
lanes plan retry demo-1 b            # un-park an item (or one node id) after fixing the cause; --all-parked for every one
lanes plan remove demo-1             # clean up and free the name; add --keep to only retire it
```

Restarting the manager is safe at any time: runs live in detached steps and all state is on disk, so a restarted
manager picks up where the last pass stopped. `stop` leaves the steps running on the hosts.

```bash
lanes manager restart     # stop, then start again with the binary now on disk (use after rebuilding bin/lanes)
lanes manager stop        # stop the manager only; steps keep running
lanes manager start
```

## 5. When something is wrong

Alarms go to `alarms.jsonl`, `lanes status` and `lanes plan status`. Each is raised once while the condition stands.
No alarm kills or resets anything; it stops new work on the host or lane.

| Alarm | Means | Do |
|---|---|---|
| `memory-pressure` | The lane block is thrashing on memory; the host takes no new work. | Wait; it clears after 3 good probes. Lower `lane.memory` or `parallel` if it repeats. |
| `host-pressure` | Host-wide memory pressure or load over its limit. Alarm only. | Check for other tenants (`lanes host status`, `top`). |
| `host-unhealthy` | Kernel network lock slow, a hung task, or the probe failed. No new work there. | Look at the reason in `lanes status`; a reboot may be needed. |
| `host-unreachable` | The manager cannot reach the host. | Check ssh. It takes work again once its preflight passes (`lanes preflight`). |
| `lane-starved` | One lane's cpu or memory pressure is over its limit; it gets no new run. | Usually a neighbour on the cores; check the reservation. Clears after 3 good ticks. |
| `lane-crash-looping` | Pods in the lane restart over and over (OOM). The lane gets no new run. | `kubectl` into that lane; raise the lane's memory or fix the app. |
| `reservation-drift` | The host's slices differ from the record; nothing is admitted. | `lanes host status` shows what differs; `lanes host reserve` it again. |
| `kernel-setting-low` | A kernel value fell below its minimum. | `lanes host reserve HOST` raises it; check for a later sysctl file. |
| `disk-floor` | Staging free space is under `min_free`; no new build. | Run `lanes gc` or free space. |
| `idle-lane` | A lane stayed free over `idle_lane` while runs wait. | Read the idle reason in `lanes status` (a gate, a held lane, a blocked edge). |
| `arm-stall` | An arm has work but nothing ended for `arm_stall`. | Look at `lanes plan status` for the blocked reason. |
| `stage` | A stage took more than `factor` x its minimum. | Open that run's log; the lane may be slow or stuck. |
| `submit` | The backend refused a release 3 passes in a row. | Read the detail (pin, host checkout, spec); fix, then `plan remove` and re-add. |

Parked items. A node that fails twice with the same reason parks its item: nothing more of it is released and its
dependents are skipped. `lanes plan status` lists each parked item and its reason. Reasons are the failure kind:
`submit`, `spec` (the template did not render), `artifact:<kind>` (a verify failed), `source-mismatch` (a run's revision
contradicts its pin), `lost` (the experiment vanished) or a run failure. Fix the cause, then `lanes plan retry NAME ITEM`
(or `NAME --all-parked`): the park is cleared and the nodes run again with their artifact inputs re-resolved; `plan status`
shows `retried at <t> (was parked: <reason>)`. A cause in the plan itself needs `plan remove` and a corrected plan (the
name is free at once); finished results stay in `plans/.removed/`.

Where things are, under the root:

```bash
lanes manager logs                         # the manager's log (bench.log)
cat alarms.jsonl                           # every alarm
ls plans/demo-1                            # state.json, results.jsonl, events, control.json
lanes status                               # experiments, lanes, hosts, last day of alarms
```

## 6. Adding a new benchmark

A benchmark plugs in as a workload adapter (the app, fault injection and grading) and a subject adapter (the system
under test), and returns a grade object with a boolean `valid`. Follow [contract.md](contract.md); fuzzops
(`benchmarks/fuzzops/`) and SREGym (`benchmarks/sregym/`) are the two worked examples.
