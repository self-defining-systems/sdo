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
query text, written in the dialect of the source it names (the task's `telemetry.sources[].dialect`). Patterns per
dialect: [PromQL](dialects/promql.md), [LogQL](dialects/logql.md). A query that returns no series yields no finding,
and a source error is a detector error, never a clear.

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

## Application instrumentation

When a health objective or incident cannot be localized with existing inputs, add instrumentation to the
application's own source as a source repair: a responder or deployer captures it as a reasoned patch
(`sdo-operator patch create`, sdo-patching skill); app files are never committed. Export request and error counters, latency
histograms and queue depth; keep labels bounded. Reflection records the need for a source repair rather than
editing source itself. Use the task's typed `telemetry` block: the source's `publish` block says whether pod
templates are annotated for scraping or OTLP HTTP metrics go to its full URL, and `references/telemetry.md` covers
bindings and capabilities. Do not infer publishing support from a query endpoint.
Write source-grounded rules with `sdo.dev/operator/sdk/metrics` in the source's dialect, declare `metrics.Watch`, and use
`metrics.NewThresholdDetector`; the controller queries and freezes results, and detectors never perform I/O.
