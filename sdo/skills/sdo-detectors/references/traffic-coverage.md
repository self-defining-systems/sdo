# Traffic coverage

Health traffic sees only what it calls. A link probe proves only that a fresh connection to a dependency opens, so a
dependency that accepts connections but fails requests stays invisible unless a continuously probed scenario sends a
request across that edge. The link detector reports an edge unreachable (consecutive failed dials), intermittent
(a failure fraction of recent dials) or slow (median dial latency far above the edge's own healthy baseline), so
declare every store edge with its protocol to make delay and loss on it visible.

- Give each service-to-service edge in `links.yaml` at least one scenario of a health-probe workload whose
  target or `DependsOn` names both ends and whose request really crosses the edge.
- Reads rarely reach services that only writes call. Include the application's own write journeys (an order, a
  booking, a post), modelled on its tests or clients, so a broken write-path dependency fails a scenario.
- Writes use dedicated synthetic data: set a `Marker` that appears in every write request, prefer an idempotent write
  (`SideEffectIdempotentWrite`, one whose repeats leave the same synthetic state) or a self-cleaning one
  (`SideEffectWriteWithCleanup` with `Cleanup` steps), and never touch real users' data.
- A scenario creates the test identities and data it needs in `Setup` steps (marked writes, run once per prober
  start; a retry resumes at the first step that has not succeeded; verify bursts reuse that setup), never relying on data set up by hand. End each with `OrExisting(status, text)`, the
  application's answer for a duplicate, so it passes on a fresh application and on one that has the data.
- Check content, not only the status: `JSONEquals` for the exact value a write chose, `ExactSet` for the exact
  set of the probe's own data, `Each` for the shape of every returned member, `Contains` for text pages. A
  write-read iteration reads back the change it just made; re-applying the same write each iteration hides lost
  state. Log in as a test identity Setup created (`SaveCookie`, `WithHeader` placeholders), never with forged
  credentials; without self-service sign-up, sign in as a probe user (`Users`) that the map's probe-identity
  procedure provisions.
- Send form bodies with `WithForm(params)` and JSON bodies with `WithJSON(template, params)`: each `{key}` in the
  template is filled from `params` or from an earlier step's `SaveJSON`, escaped for a JSON string; write `"{key}"`
  for a string and `{key}` for a number.
- Journey workloads run only on demand and never feed detection, so a write placed only there detects nothing.
- Coverage is per component (`map.coverage`): a component on the user path is covered by a probe whose operation
  enters at a user entry Service and includes it, or records `unprobed: <why>` on its map component. The link
  dials into an uncovered component are its only, reachability-strength, recovery evidence.

## Latency and saturation

A path is healthy only while it is also fast. A saturated service usually
slows its path many times over before any request fails: a CPU-throttled service moved one read's median from 7 ms to
350 ms, still under the 1.5 s default bound.

- Give each health-probe scenario its own latency bound (`slo.maxLatency`, per scenario or on the workload). The
  bound covers the whole scenario, every step together; keep it below the request `timeout`.
- Bound the median (the default `latencyPercentile: 50`); a scenario may explicitly override it. Over the default five-sample window p90 is the slowest sample, so a
  single pause fires a tight p90 bound, and a write journey's healthy tail can cross even the default (an order took
  40 ms to 1.8 s while healthy, median 170 ms). Saturation moves the median; one pause does not.
- Detect by correctness first (status, body content, read-back); a latency bound is a backstop that must never
  page on a healthy but busy host. Measure the whole scenario over at least 20 healthy iterations at the
  workload's rate, never from single requests or a quiet moment: host load moved a four-step scenario's median
  from about 100 ms to over 500 ms with nothing broken. Set the bound to the larger of three times the slowest
  healthy iteration and 1 s. A service slowed many times over still crosses it.
- `sdo-operator memory check` lists each health-probe scenario left on the default bound as a
  `traffic.default-latency` coverage gap. It never fails the check.
- Memory saturation that kills a container (OOMKilled, restarts) is already reported by `workload-health`. Latency
  and failing scenarios show the rest. With a metrics source, add threshold rules for CPU throttling and memory near
  the limit of the services on each critical path. Without one, a health detector may compare
  `program.ReadFrom(ctx).PodUsage` with a container's limits, but a failed usage read is Unknown and holds health, so
  use it only where the cluster serves resource metrics.

The default `minTimeouts: 2` requires repeated timeouts within the sample window before they contribute to either
the timeout or combined error-rate violation. Raw timeout counts and rates remain visible. Application errors
still use `maxErrorRate`. Override `minTimeouts` per scenario or workload for stricter objectives. The effective
floor is capped at the window size so a complete timeout outage always fires; one-sample windows stay strict.
Rate limits still apply after the count floor.

## Every record, every sample

The SLO judges a scenario by its error rate over its recent samples. A fault that breaks one record (one product,
one user, one key) fails only the samples that happen to pick that record, which stays under the error budget when a
step picks one of many records at random.

- Visit every seeded record on every iteration: one step per record, up to `traffic.MaxSteps` steps per scenario, and
  split a larger set across scenarios. Build the steps in a Go loop over the seeded IDs.
- After a write, read back what it changed (an order clears what it consumed, a new record is listed). Some
  failures leave the write's own response successful: a service can ignore an error from a dependency it calls only
  to clean up.

## Keep responses small

The prober reads at most `traffic.MaxResponseBody` (64 KiB) of a response. A list step whose answer grows past it
(record counts only grow) fails its check on a cut body. That is a probe defect, not an application failure: the sample
is `invalid` with defect `response_truncated`, the scenario stays unproven, and `sdo detect` and the memory check
report it with the step and the byte cap.

- Request small pages from every list step (a page size of at most 20) and assert on a record you know, never on the
  whole collection. Prefer a filter or a single-record read over a large listing.
