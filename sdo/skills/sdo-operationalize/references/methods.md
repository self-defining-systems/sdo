# Deciding and thresholds

How each detector decides, kept deterministic and cheap. Validator limits (firing counts, watches) are in the
`sdo-detector-authoring` rules and override anything here.

## Firing and severity

- Health detectors judge steady state: `firing: 2`, `clearing: 2`. Traffic detectors also default to a minimum
  continuous duration (`traffic.DefaultHealthMinDuration`); never lower it.
- A finding opens or joins an incident when its severity is at least the detector's `batching.severity`. Emit
  `critical` for user-facing symptoms and `warn` for cause checks, and set a cause detector's batching severity to
  `warn` so its findings still page. The severity then tells the responder which findings are symptoms.
- Skip objects in mid-rollout, such as a Deployment whose `observedGeneration` lags its `generation` or whose
  rollout is still progressing. The workload is legitimately between states; `workload-health` already reports a
  rollout past its progress deadline.

## Thresholds

- Declared facts are yes/no: no threshold, no tuning.
- Latency budgets come from the source. A caller's client timeout or deadline is an upper bound for the callee's
  latency, and the client's timeout bounds the journey. Without a stated budget, keep the SDK SLO defaults; do not
  invent tighter numbers.
- Error budgets: a healthy synthetic journey has zero errors. Allow only the transient rate the SDK defaults
  absorb.
- Ratios over counts: errors per request, throttled per total period, used per limit. Ratios stay valid when load
  or replica count changes.

## Metrics query patterns

The threshold detector compares one reduced value per series with a constant, so put the comparison logic in the
PromQL expression. Use the label names the exporter really sets in `Labels`.

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

## Snapshot detector patterns

- Compute every expectation from relations at detection time: owner references, selectors, volume, env and
  envFrom references, and container ports. The expectation then follows a legitimate redeploy.
- Compare siblings under the same owner, not absolute counts, for outliers.
- Use `sdk.ObservationTime(ctx)` for ages and event windows.

## Evidence

Write `Finding.Evidence` so that a responder can act without re-deriving the check:

- what was observed (object, field, value);
- what was expected, and the relation or source fact that implies it;
- for metrics, the query, value, threshold and labels (the SDK fills these in).
