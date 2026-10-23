---
name: sdo-detector-authoring
description: Author or refine deterministic Go health and incident detectors from observed resource state. Covers SDK discovery, matching and near-miss tests, and independently validated memory edits.
---

# Authoring a detector

A detector reads a Kubernetes snapshot and returns findings. Keep it deterministic: no model, environment,
clock, network access or benchmark data. Measure ages with `sdk.ObservationTime(ctx)`, never `time.Now()`. The installed `sdo-memory` skill explains ownership and logical IDs.

1. Compare the confirmed mechanism with accepted detectors. Widen one when only resource identity or a
   parameter differs; add one only for a different mechanism. The installed `service-endpoints` and
   `workload-health` detectors already cover endpoint readiness and generic pod/Deployment health.
2. Before implementing, read the [generated SDK](references/sdk.md). Keep the detector's manifest and Spec
   identical, including watches, persistence, playbook IDs and accepted provenance.
3. Derive relationships from observed objects. Use the context namespace for absent resources and emit
   stable fingerprints and role bindings. Never hard-code incident names, ports or images.
4. Prove a matching case and near misses with the [generated replay API](references/replay.md).
   The [restart-loop source](assets/restart-loop/detector.go) and
   [tests](assets/restart-loop/detector_test.go) show the current detector interface.
5. Validate final proposal files through the memory check. The broker repeats this check independently.

Read [validator rules](references/rules.md) before changing a manifest or when a diagnostic names a detector rule.

## Metrics detectors (optional input)

When the controller runs with a metrics source (`--metrics-url`, a Prometheus-compatible API), a health detector can
judge application metrics through `sdo.dev/operator/sdk/metrics`. Declare `Watches: []sdk.WatchKind{metrics.Watch}`
and an `Interval`; build the detector with `metrics.NewThresholdDetector(spec, rules)`. A rule is a query
(`metrics.Query{ID, Expr}`, plus `Range`/`Step` for a range query), a comparison, a threshold, and the label names that
map a series to a Deployment, Pod or Service. Ground each expression in metrics the application exports; never assume
a source exists.

- The runtime evaluates each declared query once per evaluation cycle at one timestamp and freezes the results into the
  snapshot. The detector does no I/O; read results with `metrics.ResultFrom` and test with `sdktest.Snapshot.Metrics`.
- An unreachable source, a failed query or a missing result is a detector error, never a clear and never a finding.
  Do not turn it into one.
- Findings carry the query, value, threshold and labels as evidence, so each series is a distinct finding.
- Query IDs are unique and non-empty, expressions non-empty, and a controller evaluates at most `metrics.MaxQueries`.
  The generated registration tests check this.
- `memory.PrepareHealth` installs the generic `metrics-alerts` detector (`metrics.NewAlertsDetector`: firing alerts of
  the application namespace, one finding each) only when a metrics URL is configured; do not author it.
