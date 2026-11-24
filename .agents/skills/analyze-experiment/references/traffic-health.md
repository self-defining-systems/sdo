# Traffic health evidence

Read the accepted traffic workload and its effective scenario SLO before classifying a probe failure.

Latency defaults to p50 (median), bounded at 1.5 s. Explicit workload/scenario `latencyPercentile` and
`maxLatency` values remain authoritative. With five samples p90 is the slowest sample, so an explicit
p90 bound can still fire on a single pause. Slow services below the default bound need a tighter
per-scenario bound; see the latency coverage section in [common failures](failure-patterns.md).

`minTimeouts` defaults to two. Below that count, timeouts stay in raw counts/rates and recent failure
evidence but do not contribute to either the timeout or combined error-rate violation. Application
errors still use the configured error budget. Repeated timeouts must also breach the configured rate.
Strict objectives may set `minTimeouts: 1`. The effective floor is capped at window size, so an entirely
timed-out window still fires and one-sample windows remain strict.

A lone timeout in raw evidence therefore does not by itself establish an active traffic finding.
Correlate the evaluated scenario, its effective bounds, and the reported violations.

### Request-metrics entry roles

A service appearing as the entry of any user operation is foreground. An entry used only by
internal operations (no port or method) is background, so request errors from it are clues.
For job counters used only to check a journey's triggered work, the map can declare
`background: true` on the entry without a separate operation. Metric names carry no role.
