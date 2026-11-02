Metrics (optional input). When the supplied task.metrics_source.query_url is non-empty,
a health detector may judge application metrics: import "sdo.dev/operator/sdk/metrics",
declare `Watches: []sdk.WatchKind{metrics.Watch}`, and build it with `metrics.NewThresholdDetector(spec, rules)`.
Each rule names a query ID and a PromQL expression you ground in metrics the application actually exports, a
comparison, a threshold, and the label names that identify its Deployment, Pod or Service. Include saturation
(CPU throttling, memory near its limit) of the services on each critical path. The controller runs each query once
per cycle; the detector never does I/O, and an unreachable source is a detector error, not a clear. Use task.metrics_source for availability and publishing instructions;
without a query URL, omit these detectors. The shared check installs the generic alerts detector itself
when a source is configured.