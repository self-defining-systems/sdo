# lanes

The benchmark-neutral lane platform: a Go module (`github.com/self-defining-systems/sdo/benchmarks/lanes`, binary
`cmd/lanes`) that runs experiment specs on isolated lanes (kind on a host, vcluster, kind in a VM, EKS) and records results.
It was imported from fuzzops-bench (see [README.md](README.md) for the source commit and what is compatible). It is
benchmark infrastructure: production `sdo/` never imports it, and it imports nothing of SDO (a test in
`tests/unit/test_architecture.py` enforces both).

## Layout

| Path | Responsibility |
|---|---|
| `cmd/lanes/` | The command: `manager` (the manager as a systemd --user unit), `serve`, `add`, `status`, `preflight`, `results`, `lane`, `host`, `ledger` and the rest of the fuzzops-bench verbs, unchanged |
| `internal/bench/` | Manager, inventory (`hosts.yaml`), specs, steps (detached, adoptable after a restart), lane state, CPU and memory ledger, preflight, results |
| `internal/bench/workload.go` | The workload adapter seam; the built-in `fuzzops` workload and the executable adapter |
| `internal/bench/hooks.go`, `preflight_subject.go` | The subject adapter seam: hook verbs and their limits; the subject's share of preflight |
| `internal/layerguard/` | Guard test and shrink-only allowlist: the lanes layer must not name SDO, apps or SDO config fields |
| `internal/plan/` | The plan engine: `plan.yaml`, nodes and their order, edges with verified artifacts, lane-free steps, lock and state on disk; talks to the platform through a `Backend` (real one: `internal/bench/planbackend.go`) |
| `internal/{env,lanelock,miniyaml}/` | Lane environment and kubectl/kind processes, the per-lane flock, the JSON/YAML-subset decoder |
| `deploy/` | Lane scripts (kind-lane, vcluster-lane, lane-limits, node-preload, registries, image delivery) and the root helper under `deploy/host/` |
| `docs/contract.md` | The adapter contract: environment in, last stdout line JSON out; verbs; unsupported handling |
| `docs/plan.md` | The `plan.yaml` schema and the plan commands |
| `docs/eks.md` | Provider `eks`: a lane is an EKS cluster; sizing from the footprint, quota and budget admission, cost ledger, ECR image delivery, and the host-safety gates it skips |

## Adapters

Adapters stay with their benchmark and plug in through the contract:

- [`benchmarks/fuzzops/`](../fuzzops/AGENTS.md): the SDO subject adapter for fuzzops app packs (`hooks.py`); the
  fuzzops workload is built in (`bin/fuzzops`).
- [`benchmarks/sregym/`](../sregym/AGENTS.md): SREGym; its workload adapter is planned (not part of this module yet).

## Build and test

```bash
source /mnt/data/shli/cache/env.sh
cd benchmarks/lanes && go build -o bin/lanes ./cmd/lanes && go test -tags=integration ./...
```

CI runs it through `scripts/test_go_ci.sh` (a workspace module) and `scripts/check_go_mains.sh` (`go vet`). Tests use a
fake `kind`, `kubectl` and `fuzzops` (`internal/bench/testdata`) and stub docker/kind in `deploy/`; they run real
processes and shell scripts, so every test file carries the `integration` build tag (the ratchet exempts such tests) and
the non-test code of the packages here is a determinism-ratchet edge (`sdo/operator/internal/purity/determinism`:
host processes, ssh and clocks).

## Rules

- Compatibility first: do not change verbs, flags, `hosts.yaml`, the spec format or the root layout without a
  migration; a running fuzzops-bench root must stay servable.
- No benchmark names in the platform logic; benchmark specifics live behind a workload or subject adapter. The one
  exception today is the built-in `fuzzops` workload and the SDO operator's preflight default
  (`preflight_subject.go`), kept so existing specs behave identically until their adapters implement the verbs.
- Every wait has a deadline and a named error; the platform retries, adapters never do.

## Isolation

Lane isolation on a host is imported intact and behavior-identical; phase 1 adds no cgroup features.

- `deploy/lane-limits.sh` pins a lane's node containers to whole cores (SMT siblings together) and a memory share, and reads the result back.
- `deploy/lane-docker-shim/docker` wraps `docker` so the kind nodes of a lane start under the lane's limits.
- `deploy/host/` is the one-time root setup: `eval.slice` (the parent cgroup of every lane, `EvalSlice` in `internal/bench/lanestep.go`), the `sdo-eval-reserve` helper that holds the exclusive partition and repins runc `AllowedCPUs`, its gc timer and the sudoers rule.
- `internal/bench` keeps the per-user core and memory ledger (`/var/tmp/fuzzops-lanes-<uid>`, `cpualloc.go`, the `ledger` verb), splits memory across nodes, and counts `oom_kill` from the lane's cgroup `memory.events` per run: a run with OOM kills is invalid unless the spec sets `accept_oom_kills`.

### Core reservation (config plus commands)

Nobody types `systemctl`. A host declares its lane block in hosts.yaml and the commands apply it through the root helper
(`deploy/host/lane-reserve`; `sdo-eval-reserve` stays as an alias path):

```yaml
hosts:
  spokane1:
    reserve: {cpus: "88-111,200-223", memory: 400Gi, host_reserve_cpus: 2}   # or {cores: 24, numa: 3}
    isolation: one-way-ok     # optional: allow lanes on this host with no active reservation (rows are flagged)
```

| command | does |
|---|---|
| `lanes host install-helper HOST [--sudo-password-file PATH]` | the only sudo step: installs the helper, the `lanes.slice` unit and a sudoers rule limited to the helper. The script is copied to a temp file on the host and run as `sudo bash FILE` over `ssh -t`, so a human types the password once per host; `--sudo-password-file` feeds `sudo -S` from the file (no tty) for tools |
| `lanes host reserve HOST\|all` | shrinks `user.slice`, `system.slice`, `machine.slice` and `kubepods.slice` to the complement of the block and puts `lanes.slice` on it; idempotent; prints before and after; the replaced values are saved on the host in `/var/lib/lane-reserve/block` (persistent: systemd keeps the reservation across a reboot; a record an older helper kept under `/run/sdo-eval-reserve` is moved there) |
| `lanes host unreserve HOST [--drain]` | restores the saved values; refuses while lanes run unless `--drain` |
| `lanes host status [HOST]` | the block, two-way or one-way, foreign containers pinned inside it, our containers outside it, drift |
| `lanes host adopt HOST [--cpus LIST] [--prev SLICE=LIST]...` | records a reservation made by hand (nothing is changed) and saves the slices' values from before it (unrestricted unless `--prev`, repeatable, `SLICE=` for unrestricted), so a later reserve or unreserve has them |

The bench records the active block in `<root>/reservations/<host>.json`. `max_load5` is optional (0 or omitted: no load gate, load5 is only reported) on a host with `reserve:`; a host without one still needs it above 0.
Admission: every lane cpuset is a subset of the
block; a host with no reservation admits no lanes unless the spec (`isolation: one-way-ok`) or the host says so, and
those rows carry `isolation.mode: one-way-ok` (a `one-way` row ran on a block whose other slices still use it; a spec can
require `isolation: two-way`). Every reconcile (at most once a minute per host) compares the live slices with the record;
a mismatch or an unreadable host raises an alarm (`alarms.jsonl`, `bench.log`) and stops admission there.
Every step of a reserved host starts as `sudo -n --preserve-env lane-reserve lane-exec LANE -- lanes step ...`, which
moves the step's process into `lanes.slice/lanes-<lane>.slice` before it runs, so builds, the agent, hooks and kubectl are
charged to the lane; the lane's kind nodes go to the same slice through the docker shim's `--cgroup-parent`.
Tests use a fake systemctl and a fake cgroupfs (`deploy/laneblock_test.go`) and a fake helper (`internal/bench/reserve_test.go`);
CI never runs the real helper.

Next, as a separate PR: IO and pids limits, per-lane PSI, and a two-way slice that also protects lanes from other work on the host.

Neutrality notes (not redesigned in phase 1): the slice name `eval.slice`, the helper and unit names `sdo-eval-reserve*`, the `FUZZOPS_LANE_*` and `FUZZOPS_EVAL_HELPER` variables and the ledger directory names are SDO and fuzzops era names. Hosts have them installed, so renaming needs a coordinated host change (design: M1).
