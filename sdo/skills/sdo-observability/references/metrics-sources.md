# Metrics sources

`metrics.Query.Source` names the telemetry source that answers a query. `Expr` is the query text in that source's
dialect (PromQL for a Prometheus source); the runtime passes it through unparsed and never rewrites it.

- Leave `Source` empty to use the default metrics source. Detectors written before sources existed keep working.
- A name must match `^[a-z][a-z0-9-]{0,30}$` and be bound. Only name a source the task context lists.
- Resolution: a named source must be bound and serve metrics; an empty name selects the source marked default, else the
  only source serving metrics; several without a default is ambiguous.
- Unbound, ambiguous or unreachable sources are detector errors that name the source and list what is bound, for
  example `telemetry source "prom" is not bound; bound: default`. They never clear and never produce a finding.
- One query ID must mean one definition: the same `ID` with two different `Source` values conflicts.
- Each frozen `metrics.Result.Query` carries the query as declared, including `Source`. Findings from a threshold rule
  with a source carry `source` in their metadata. In `sdktest`, set `Source` on the `Query` the fixture uses.
- Alerts: `metrics.Alert.Source` is set by the runtime. The generic `metrics-alerts` detector reads the merge of every
  alert-capable source (any failing source fails the read), and an alert reported by two sources is one finding.
