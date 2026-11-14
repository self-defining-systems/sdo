Application telemetry (optional input). task.telemetry lists the bound sources, each with its kinds and query
dialect. A health detector may judge a bound metrics source: import "sdo.dev/operator/sdk/metrics", declare
`Watches: []sdk.WatchKind{metrics.Watch}`, and build it with `metrics.NewThresholdDetector(spec, rules)`.
Each rule names a query ID, a source ID (omit it only for the default source), an expression in that source's
dialect grounded in what the application exports, a comparison, a threshold, and the label names that identify
its Deployment, Pod or Service. Cover saturation of the services on each critical path. The controller runs each
query once per cycle; the detector never does I/O, and an unreachable source is a detector error, not a clear.
Never query an unbound source; with no bound source, omit these detectors. Read task.telemetry.mode for whether
you may bind sources (sdo-observability references/telemetry.md). The shared check installs the alerts detector.