# deploy/host: the optional root helper for lane limits

The bench's lane limits (docs/bench.md, "Lane limits") need **no root**: each lane's node containers get their own whole
cores and memory when the lane is built. This directory is only for hosts shared with other work, where the lanes' cores should
also be kept free of everything else while a measured run is active. A human with root installs it once per host:

```bash
sudo install -m 0755 deploy/host/sdo-eval-reserve /usr/local/sbin/sdo-eval-reserve
sudo install -m 0644 deploy/host/eval.slice /etc/systemd/system/eval.slice
sudo install -m 0644 deploy/host/sdo-eval-reserve-gc.service deploy/host/sdo-eval-reserve-gc.timer /etc/systemd/system/
sed 's/BENCHUSER/<the bench account>/' deploy/host/sdo-eval-reserve.sudoers | sudo tee /etc/sudoers.d/sdo-eval-reserve   # then: sudo chmod 0440 ... ; sudo visudo -c
sudo systemctl daemon-reload && sudo systemctl enable --now sdo-eval-reserve-gc.timer
sudo systemctl start eval.slice
```

and sets `lanes: {cpus: "<range>", reserve: true}` for the host in hosts.yaml. Needs cgroup v2 with the cpuset controller
(`cat /sys/fs/cgroup/cgroup.controllers`), `docker` for root and Docker's systemd cgroup driver (`docker info | grep -i cgroup`).

`sdo-eval-reserve on LANE --cpus LIST [--lease 10m] [--memory-min SIZE]` / `off LANE` / `status` / `gc` / `repin` / `pin ID LIST`: while any lane holds a
live lease, `eval.slice` is a root cpuset partition (`cpuset.cpus.partition=root`) on the union of the live leases' lists: the kernel takes those cores
away from every other cgroup, so nothing else runs on them; `memory.min` protects the lanes' memory. The bench renews a lease every 3 minutes while a run
lasts; one that is not renewed expires after 10 minutes (the `gc` timer runs every minute). The last `off`, or the expiry of the last lease, makes the slice
a plain member again and puts back the `memory.min` an administrator had set. Every `on`, `gc` and `repin` also re-checks each node scope under
`eval.slice` against what docker says its cpuset is and fixes it (the runc `AllowedCPUs` bug, see docs/bench.md).

The helper is the policy point: a CPU list must be within the host's online CPUs and leave at least one online CPU outside it, `--memory-min` is bounded by
the host's RAM, numbers are length-checked before any arithmetic, lane names, container ids, durations and sizes are regex-checked, and the `SDO_EVAL_*` test
hooks are only read when it is not run as root. The sudoers rule allows the one path and nothing else.

**What reserving does to the rest of the host:** while a lease is live, every cgroup outside `eval.slice` (other users' containers and jobs, the registry,
lanes built before lane limits) is confined to the remaining CPUs, and a task pinned to the reserved cores with `sched_setaffinity` has its mask reset by the
kernel and not restored at `off`. `sdo-eval-reserve status` lists the containers displaced. On a shared host, agree on it with the other users first.

Lanes that were built before this existed live under `system.slice`, not `eval.slice`, and are not reserved: recreate them
(or keep `reserve: false` for them). Docker's `--cgroup-parent` can only be chosen when a container is created.

**Persistent block, slices that appear later:** `block reserve` puts every top-level slice (user, system, machine, kubepods) on the complement of the
block. A slice that is not a systemd unit at that time (kubepods.slice is created at boot by a k3s agent) gets a persistent drop-in
(`/etc/systemd/system/<slice>.d/50-lane-reserve.conf`) and, if its cgroup exists, a direct `cpuset.cpus` write. `gc` (every minute) re-applies the
reservation to any slice found overlapping the block. `block status` reports `two-way yes` only when every slice's systemd property and its cgroup's
cpus stay outside the block; an overlapping slice is named (`overlap SLICE cpuset=...`). Re-install the helper (`lanes host install-helper`) on a host
to get this.
