---
name: sdo-detector-authoring
description: Use when authoring or changing an SDO detector - a deterministic Go health or incident detector under operational memory. Gives the memory new -> write -> go test -> memory check workflow, the Go SDK reference, the validator rules, and a complete example.
---

# Authoring a detector

A detector is deterministic Go that reads a Kubernetes snapshot and returns findings. It never calls an LLM, reads
the environment, the clock, or the network, and never looks at benchmark data or verdicts. Measure ages with
`sdk.ObservationTime(ctx)`, never `time.Now()`. Read the `sdo-memory` skill first for artifact IDs and the
`memory` commands.

## Workflow

1. Look before you add: `sdo-operator memory ls --kind detector`. Widen an existing detector when only resource
   identity or a parameter differs. Add a new one only for a different failure mechanism. The Go-installed
   `service-endpoints` and `workload-health` health detectors already report Services without ready endpoints and
   generic pod/Deployment state (failed, pending, crash-looping, image-pull, OOMKilled, unready, unavailable).
2. Scaffold: `sdo-operator memory new detector <id> --class incident --playbook <playbook-id>` (health judge:
   `--class health`). It writes `manifest.yaml`, `detector.go`, and `detector_test.go`.
   Create the playbook first if it does not exist (see `sdo-playbook-authoring`).
3. Write the detector. Edit `manifest.yaml` and the `Spec()` in `detector.go` together: they must agree field for
   field. The API is in [SDK reference](references/sdk.md); a full example is in `assets/restart-loop/`.
4. Write tests: one case that fires (use different resource names than the incident that taught you the pattern)
   and near-miss cases that stay quiet. Use `sdktest.Snapshot` and consult the [replay reference](references/replay.md).
5. Run `go test` in the detector directory when your sandbox allows `go` and the checkout resolves the SDK
   module. If `go` is blocked or cannot resolve `sdo.dev/operator/sdk`, do not work around it: the next step
   builds, vets, and runs the same tests.
6. Run `sdo-operator memory check`. Fix every diagnostic by rule ID (see [validator rules](references/rules.md)) and
   repeat until it exits 0.

## Principles

- Detect the mechanism, not the incident: derive what a check depends on from observed objects (selectors, owner
  references, volume and env references), never from hard-coded names or the literal namespace. Use
  `DetectionContext.Namespace()` for resources that are absent.
- Refer to playbooks by logical ID in `Spec().Playbooks`, `Finding.Playbooks`, and `possiblePlaybooks`.
- Emit stable fingerprints and `ParameterBindings` whose keys match the playbook's placeholders in lower case
  (`target_resource` for `<TARGET_RESOURCE>`).
- Read [validator rules](references/rules.md) before changing a manifest; read [SDK reference](references/sdk.md) before using an SDK type.

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
