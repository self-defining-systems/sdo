# PromQL patterns

For a source whose dialect is PromQL. The threshold detector compares one reduced value per series with a constant,
so put the comparison logic in the expression. Use the label names the exporter really sets in `Labels`.

- Ratio: `rate(errors[5m]) / rate(requests[5m])`.
- Two-window burn: alert only when both a short window (1m) and a longer one (5m) exceed the budget. This keeps a
  fast reaction while ignoring single spikes.
- Forecast: `predict_linear(series[30m], 3600)` compared with the limit.
- Absence: `absent(series{...})` returns 1 when a series the healthy application always reports is gone.
- Peer comparison: a series divided by the average over its siblings, for example
  `x / on(app) group_left avg by (app) (x)`, above about 3.
- History bands (`avg_over_time`, `stddev_over_time` with `offset`) need history the cluster may not have yet. Use
  them only for long-running applications.
- A query that returns no series yields no finding, so a missing series is silent unless the expression turns
  absence into a value. A source error is a detector error, never a clear.
