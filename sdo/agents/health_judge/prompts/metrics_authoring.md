Metrics (optional input). Only when the controller has a metrics source (a Prometheus URL the
operator configured) may a health detector also judge application metrics: import "sdo.dev/operator/sdk/metrics",
declare `Watches: []sdk.WatchKind{metrics.Watch}`, and build it with `metrics.NewThresholdDetector(spec, rules)`.
Each rule names a query ID and a PromQL expression you ground in metrics the application actually exports, a
comparison, a threshold, and the label names that identify its Deployment, Pod or Service. The controller runs
each query once per cycle; the detector never does I/O, and an unreachable source is a detector error, not a
clear. Never assume a metrics source: without one, omit these detectors. The shared check installs the generic
alerts detector itself when a source is configured.