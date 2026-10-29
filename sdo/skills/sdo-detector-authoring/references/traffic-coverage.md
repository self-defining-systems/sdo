# Traffic coverage

Health traffic sees only what it calls. A link probe proves only that a fresh connection to a dependency opens, so a
dependency that accepts connections but fails requests stays invisible unless a continuously probed scenario sends a
request across that edge.

- Give each service-to-service edge in `links.yaml` at least one scenario of the `health` (health-probe) workload whose
  target or `DependsOn` names both ends and whose request really crosses the edge.
- Reads rarely reach services that only writes call. Include the application's own write journeys (an order, a
  booking, a post), modelled on its tests or clients, so a broken write-path dependency fails a scenario.
- Writes use dedicated synthetic data: set a `Marker` that appears in every write request, prefer an idempotent write
  (`SideEffectIdempotentWrite`, one whose repeats leave the same synthetic state) or a self-cleaning one
  (`SideEffectWriteWithCleanup` with `Cleanup` steps), and never touch real users' data.
- Send form bodies with `WithForm(params)` and JSON bodies with `WithJSON(template, params)`: each `{key}` in the
  template is filled from `params` or from an earlier step's `SaveJSON`, escaped for a JSON string; write `"{key}"`
  for a string and `{key}` for a number.
- Journey workloads run only on demand and never feed detection, so a write placed only there detects nothing.
- `sdo-operator memory check` lists each edge no health-probe scenario drives as a `traffic.undriven-edge` coverage
  gap. It never fails the check. Drive the edge, or write a comment above that link in `links.yaml` saying why no
  synthetic request can safely cross it.

## Latency and saturation

A path is healthy only while it is also fast. A saturated service (CPU at its limit, a cache out of memory) usually
slows its path many times over before any request fails: a CPU-throttled service moved one read's median from 7 ms to
350 ms, still under the 1.5 s default bound.

- Give each health-probe scenario its own latency bound (`slo.maxLatency`, per scenario or on the workload). The
  bound covers the whole scenario, every step together; keep it below the request `timeout`.
- Bound the median (the default `latencyPercentile: 50`); a scenario may explicitly override it. Over the default five-sample window p90 is the slowest sample, so a
  single pause fires a tight p90 bound, and a write journey's healthy tail can cross even the default (an order took
  40 ms to 1.8 s while healthy, median 170 ms). Saturation moves the median; one pause does not.
- Base the bound on what the path does: a read that crosses one or two in-cluster services has a median of tens of
  milliseconds, so a median bound of 100 to 250 ms leaves room for a loaded cluster and still sees a service slowed
  many times over. Give multi-service writes about a second.
- `sdo-operator memory check` lists each health-probe scenario left on the default bound as a
  `traffic.default-latency` coverage gap. It never fails the check.
- Memory saturation that kills a container (OOMKilled, restarts) is already reported by `workload-health`. Latency
  and failing scenarios show the rest. With a metrics source, add threshold rules for CPU throttling and memory near
  the limit of the services on each critical path. Without one, a health detector may compare
  `program.ReadFrom(ctx).PodUsage` with a container's limits, but a failed usage read is Unknown and holds health, so
  use it only where the cluster serves resource metrics.

The default `minTimeouts: 2` requires repeated timeouts within the sample window before they contribute to either
the timeout or combined error-rate violation. Raw timeout counts and rates remain visible. Application errors
still use `maxErrorRate`. Override `minTimeouts` per scenario or workload for stricter objectives; use `1` for a
one-sample window. Rate limits still apply after the count floor.

## Every record, every sample

The SLO judges a scenario by its error rate over its recent samples. A fault that breaks one record (one product,
one user, one key) fails only the samples that happen to pick that record, which stays under the error budget when a
step picks one of many records at random.

- Visit every seeded record on every iteration: one step per record, up to `traffic.MaxSteps` steps per scenario, and
  split a larger set across scenarios. Build the steps in a Go loop over the seeded IDs.
- After a write, read back what it changed (an order clears what it consumed, a new record is listed). Some
  failures leave the write's own response successful: a service can ignore an error from a dependency it calls only
  to clean up.
