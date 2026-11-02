---
name: sdo-detector-authoring
description: Author or refine deterministic Go health and incident detectors from observed resource state. Covers SDK discovery, matching and near-miss tests, and independently validated memory edits.
---

# Authoring a detector

<<<<<<< HEAD
A detector is deterministic Go that reads a Kubernetes snapshot and returns findings. It never calls an LLM, reads
the environment, the clock, or the network, and never looks at benchmark data or verdicts. Measure ages with
`sdk.ObservationTime(ctx)`, never `time.Now()`. Read the `sdo-memory` skill first for artifact IDs and the
`memory` commands. To decide which detectors an application needs, use the `sdo-operationalize` skill.
=======
A detector reads the runtime snapshot and returns findings. Keep it deterministic: no model, environment,
clock, network access or benchmark data. Measure ages with `sdk.ObservationTime(ctx)`, never `time.Now()`. Read the [memory principles](../sdo-memory/SKILL.md#memory-principles) for evidence, reuse and generalization; that skill also explains ownership and logical IDs.
>>>>>>> origin/vic-lsh/dev/sdo-improve

1. Compare the confirmed mechanism with accepted detectors using the memory principles. The installed `service-endpoints` and
   `workload-health` detectors cover generic symptoms; their grace periods do not replace prompt cause-specific coverage.
   When generic detection lagged a visible cause, watch the resource read by the predicate and fire within seconds; refine an existing matching incident detector first. Health detectors stay judge-owned.
   `pod-admission` checks a frozen server-side dry-run Pod create via `ctx.PodAdmission()`; API rejections fire,
   while missing observations and transport failures are errors. Test with `sdktest.Snapshot.Admission`.
2. Before implementing, read the [generated SDK](references/sdk.md). Keep the detector's manifest and Spec
   identical, including watches, persistence, playbook IDs and accepted provenance.
3. Derive relationships from observed objects. Use the context namespace for absent resources and emit
   stable fingerprints and role bindings. Observed evaluation supplies an omitted `DetectorID` from
   the emitting detector's `Spec.ID`; an authored identity must still match that detector.
<!-- replay:on -->
4. Explore matching cases and near misses with the [generated replay API](references/replay.md).
<!-- /replay:on -->
   Replay and test verdicts are information, never a save gate; keep unavailable observations explicit.
   The [restart-loop source](assets/restart-loop/detector.go) and
   [tests](assets/restart-loop/detector_test.go) show the current detector interface.
5. Use shell tools to inspect and edit the proposal; run the complete memory check before returning. The broker repeats it independently.

<<<<<<< HEAD
1. Look before you add: `sdo-operator memory ls --kind detector`. Widen an existing detector when only resource
   identity or a parameter differs. Add a new one only for a different failure mechanism. The Go-installed
   `service-endpoints` and `workload-health` health detectors already report Services without ready endpoints and
   generic pod/Deployment state (failed, pending, crash-looping, image-pull, OOMKilled, unready, unavailable).
2. Scaffold: `sdo-operator memory new detector <id> --class incident --playbook <playbook-id>` (health judge:
   `--class health`). It writes `manifest.yaml`, `detector.go`, and `detector_test.go`.
   Create the playbook first if it does not exist (see `sdo-playbook-authoring`).
3. Write the detector. Edit `manifest.yaml` and the `Spec()` in `detector.go` together: they must agree field for
   field. The API is in [SDK reference](references/sdk.md); a full example is in `assets/restart-loop/`. For
   synthetic traffic scenarios, workloads and links, read the [traffic reference](references/traffic.md).
4. Write tests: one case that fires (use different resource names than the incident that taught you the pattern)
   and near-miss cases that stay quiet. Use `sdktest.Snapshot` and consult the [replay reference](references/replay.md).
5. Run `go test` in the detector directory when your sandbox allows `go` and the checkout resolves the SDK
   module. If `go` is blocked or cannot resolve `sdo.dev/operator/sdk`, do not work around it: the next step
   builds, vets, and runs the same tests.
6. Run `sdo-operator memory check`. Fix every diagnostic by rule ID (see [validator rules](references/rules.md)) and
   repeat until it exits 0.
=======
Before returning memory edits, run the complete memory check and resolve every diagnostic until it passes.
>>>>>>> origin/vic-lsh/dev/sdo-improve

Read [validator rules](references/rules.md) before changing a manifest or when a diagnostic names a detector rule.

## Runtime reads

Use `program.ReadFrom(ctx)` for the shared detector/Diagnose surface; read the
[generated program SDK](../sdo-playbook-authoring/references/program-sdk.md) before using it.
Use the [recorded Kubernetes client](../sdo-playbook-authoring/references/kubernetes-client.md) for typed or dynamic API reads.
Detectors can call `ctx.DiffAgainstSource(ref)` directly, before playbook selection.
`Changed` covers field drift and source-absent objects; `SourceAbsent` requires a successful accepted-source lookup.
Bind playbooks through findings. The shared view adds `LogsWithOptions` (previous instances), `PodUsage`/`NodeUsage`, probes, baseline diffs, metric queries, alerts and traffic.
The runtime caches identical reads per snapshot and records actual results and failures for offline replay.
Logs are capped at 64 KiB / 5 s; probes at 5 s. Read failures are errors/Unknown, never clearance;
a successful probe with `Success:false` is an observed failure. Keep always-on reads cheap and use Diagnose for depth.
Test using `sdktest.Snapshot.Readings` plus phase recordings; never fill absent evidence with healthy answers.

## Synthetic traffic confirmation

Steady traffic remains pending until every scenario has succeeded since observation began and has enough recent usable SLO samples. Pending carries a reason naming unproven scenarios, establishes no health and never opens an incident by itself. Qualified violations still fire alongside unproven peers. Process restarts lose qualification; recovery requires fresh observations and the controller's ordinary clearing check. A verify burst reports present scenario evidence, while controller verification owns incident closure.

Health traffic sees only what it calls: give every `links.yaml` edge a continuous scenario that crosses it, writes
included, bound each scenario's latency and cover every seeded record; see [traffic coverage](references/traffic-coverage.md).

## Logs and usage detectors

A detector that reads container logs implements `logs.Consumer`: `LogQueries()` returns its queries (an ID, RE2
patterns with their own IDs, and `Previous` for a crashed container's tail). Declare `logs.Watch`, read with
`logs.Require(ctx, queryID)`, and decide with `logs.Appeared`, `logs.Rate` or `logs.Silent`, which take a pattern
ID. A detector that reads kubelet usage implements `usage.Consumer`: `UsageRequirements()` returns
`usage.Requirements{Throttling: true}` only when it needs CPU throttling. Declare `usage.Watch` and read with
`usage.From(ctx)`. A missing input or a source error is a detector error, never a clear. Test with
`sdktest.Snapshot.Logs` and `sdktest.Snapshot.Usage` (see the [replay reference](references/replay.md)).

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
- Query IDs are unique and non-empty, expressions non-empty, and a declared query bundle contains at most `metrics.MaxQueries`.
  The generated registration tests check this.
- `memory.PrepareHealth` installs the generic `metrics-alerts` detector (`metrics.NewAlertsDetector`: firing alerts of
  the application namespace, one finding each) only when a metrics URL is configured; do not author it.

## Generic reachability baseline

The controller's opt-in healthy-baseline gate measures exclusions on each cluster and keeps them as that cluster's own state, outside memory. Never write measured values from one cluster (Service UIDs, allocated IPs, probe or capture times) into `.sdo/`: memory holds what to measure and thresholds, not one cluster's measurement, and the check rejects such fields (`memory.cluster-local`). Do not infer exclusions from missing successful dials or author them from an incident. Generic reachability invalidates an exclusion when source, objective or target configuration changes; new wrong targetPort failures remain detectable. Service UID/resourceVersion, allocated ClusterIP/IP-family values, NodePort and health-check NodePort do not identify the application configuration; namespace/Service/port identity, headless routing, selector, targetPort/protocol, selected container configuration, referenced ConfigMaps and selecting network policies do. Inspect `baseline_status` in finding port metadata when an earlier exclusion no longer applies.

For kinds outside the typed getters, use the [generic object interface](references/generic-objects.md).

The controller's verified onboarding window also records immutable measured reachability reference evidence beside the final onboarding record. Consistently refused, never-served declared ports are visible `declared-but-never-served` observations rather than health findings. Unconfirmed probe windows are pending, not clear. Previously serving ports retain the ordinary failure gate; target configuration changes and later successful dials end an exclusion. Agents cannot author or expand this controller-owned install reference.
