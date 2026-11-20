# Real-host smoke test

`smoke/smoke.sh` runs the real manager against one real reserved host with one small lane and checks what hermetic
tests cannot: the root helper, cgroup v2 limits, the ledger and cluster lifecycle, a second workload. **Run it after
every change to `benchmarks/lanes`** (and before a campaign that depends on a lanes change). It takes about 5 to 8
minutes plus any wait for cores.

```bash
benchmarks/lanes/smoke/smoke.sh --host spokane1 --root /mnt/data/shli/cache/smoke-root
#   [--host-dir DIR]              scratch directory on the host (default for known hosts)
#   [--sudo-password-file FILE]   accepted for hosts whose root helper is missing; never read otherwise, never printed
#   [--run-seconds N]             how long the tiny fault is held (default 45)
#   [--wait-cores SECONDS]        how long to wait for 2 free cores of the block (default 600)
#   [--keep]                      keep the scratch directories after the run
```

The host is the `--host` argument: spokane1, chelan4 (default `--host-dir` known for both) or any other host that has a
reserved block (`lanes.slice`) and the root helper, given `--host-dir`. The block must have 2 free cores in the ledger.

Output is `PASS`/`FAIL name reason` per step and a final line with the runtime. Exit status is 1 on any FAIL. The log of
every command is `<root>/smoke.log`, the manager's `<root>/manager.log`.

## What it does

Everything it makes is named `smk-*`: lane `smk-1`, plan `smk-plan`, and a scratch root and host directory. Every
other cluster, lane and ledger entry is foreign and is never touched. The lane's 2 cores come from the host ledger
through the allocator, so it shares the reserved block safely with live lanes; if the block is full it waits
(`--wait-cores`) and then fails with the allocator's reason.

The workload is `smoke/adapter.sh` (a throwaway git checkout the script makes on each host): two pause pods, a fault
that scales them to 0, and a subject that does nothing. It talks to lanes only through the workload and subject verbs
(`docs/contract.md`).

| step | what is checked |
|---|---|
| `host-status`, `preflight`, `plan-add` | `lanes host adopt/status`, `lanes preflight`, `lanes plan add` on the generated inventory and plan |
| `lane-build` | a kind lane is built through `kind-lane.sh` from the installed platform and the p1 run step starts |
| `lane-cgroup` | the step runs in the `step` leaf of the lane slice, the slice itself holds no process, and every node container has a memory limit, the lane's cgroup parent and cores inside the lane's ledger cores, which are inside the block |
| `step-kill-retry`, `step-recover` | p1's run step is killed with SIGKILL; the item is retried as `-x1` and the lane is recovered and reused |
| `artifact-edge`, `manager-restart` | p1's artifact is verified and p2 starts; the manager is restarted mid-run and adopts the step that kept running, and the plan completes |
| `artifact-seeded` | p2's run record shows it saw p1's artifact as its seed |
| `release` | after `plan remove` the cluster is deleted and the ledger entry released |

Cleanup runs on every exit (trap): stops the manager (SIGKILL after 20 s, noted in the log), removes the plan, kills
leftover `smk-1` steps, deletes the `smk-1` cluster, drops its ledger entry, and removes the scratch directories.

## Known gaps

- The root helper leaves an empty `lanes-smk_1.slice` cgroup directory; it is reused by the next run.
- The subject preflight and the platform's baseline memory check overlap, and the platform names the workload
  "fuzzops" in a few messages. Both are tracked by the layering work, not by the smoke.
