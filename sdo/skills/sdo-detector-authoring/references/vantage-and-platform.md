# Vantage probes and the platform observation

## Vantage probes

A vantage probe reads a Pod from the inside without depending on the application image: the controller attaches a
helper container (pinned BusyBox) that shares the target container's process namespace and the Pod's network
namespace, and runs one fixed read per kind. Implement `vantage.Consumer` and watch `v1 Pod`; `Detect` never execs.

| Kind | Reads | Typical fault |
|---|---|---|
| `tcp-connect`, `http-get` | fresh dials of a peer from the client Pod | client-side delay or loss |
| `qdisc` | queueing disciplines (`netem`, `tbf`) | traffic shaping on the Pod |
| `proc-limits` | open-files and process limits, open descriptors | a lowered `RLIMIT_NOFILE` |
| `proc-state` | processes by state | a stopped (SIGSTOP) or zombie process |
| `cgroup-cpu`, `cgroup-memory`, `cgroup-io` | `cpu.max`, `memory.*`, `io.max`, pressure | a CPU or IO limit written into the cgroup |
| `shm-usage` | `/dev/shm` size and use | a full shared-memory tmpfs |
| `socket-summary` | TCP sockets by state and peer | a connection flood or leak |

- `vantage.NewDetector(spec, probes...)` judges state kinds with `vantage.Judge` against the promoted healthy
  reading (`Result.Baseline`). `vantage.NewClientLinkDetector` dials a `links.yaml` edge from the client and judges it
  with the link rules; its finding has `vantage: client`, and `impairment: client-egress` when the prober sees the edge
  healthy.
- Facts are named by the `vantage.Fact*` constants. `vantage.Require` errs for a missing, failed or still-starting
  probe; never turn that into a clear.
- Limits: 30 second minimum interval, 16 Pods and 16 probes, 6 second timeout, 4 dials per run.
- Test with `sdktest.Snapshot.Vantage`.

## Platform observation

Implement `sdk.PlatformConsumer` (`ConsumesPlatform() bool { return true }`) and read `sdk.Platform(ctx)`: the
Namespace's labels and annotations, LimitRanges, ResourceQuotas with usage, Roles, RoleBindings, every admission
webhook that selects Pods in the namespace (named in source or not, with timeout and objectSelector), and the
accepted source inventory. `MissingFromLive(ctx, kinds...)` names source objects that are not live; `Changes()`
lists what differs from the healthy baseline. An unreadable part is an error from its accessor. Test with
`sdktest.Snapshot.Platform`.

`ctx.PodAdmission()` also records `Latency`, the `Mutations` admission made to a template-built Pod and the
`Webhooks` whose rules, namespaceSelector and objectSelector select it; one mutating candidate attributes a mutation.
