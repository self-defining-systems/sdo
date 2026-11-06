---
name: sdo-detector-authoring
description: Author or refine deterministic Go health and incident detectors from observed resource state. Covers SDK discovery, matching and near-miss tests, and independently validated memory edits.
---

# Authoring a detector

A detector reads the runtime snapshot and returns findings. Keep it deterministic: no model, environment,
clock, network access or benchmark data. Measure ages with `sdk.ObservationTime(ctx)`, never `time.Now()`. Read the [memory principles](../sdo-memory/SKILL.md#memory-principles) for evidence, reuse and generalization; that skill also explains ownership and logical IDs. To decide which detectors an application needs, use the `sdo-operationalize` skill.

1. Compare the confirmed mechanism with accepted detectors using the memory principles. The installed `service-endpoints` and
   `workload-health` detectors cover generic symptoms; their grace periods do not replace prompt cause-specific coverage.
   A playbook with a program ships with a detector on its cause ([how](references/cause-specific.md)); refine a matching one first. Health detectors stay designer-owned.
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
   The [restart-loop source](assets/restart-loop/detector.go) and [tests](assets/restart-loop/detector_test.go) show the interface.
5. Use shell tools to inspect and edit the proposal; run the complete memory check before returning. The broker repeats it independently.

For synthetic traffic scenarios, workloads and links, read the [traffic reference](references/traffic.md).

Read [validator rules](references/rules.md) before changing a manifest or when a diagnostic names a detector rule.

## Cause-specific detectors

A new or changed playbook with a program ships with an incident detector keyed on its cause, unless only the generic
symptom exists (say so in the outcome reason): [cause-specific detectors](references/cause-specific.md).

## Runtime reads

Use `program.ReadFrom(ctx)` for the shared detector/Diagnose surface; read the
[generated program SDK](../sdo-playbook-authoring/references/program-sdk.md) before using it.
Use the [recorded Kubernetes client](../sdo-playbook-authoring/references/kubernetes-client.md) for typed or dynamic API reads.
Detectors can call `ctx.DiffAgainstSource(ref)` directly.
`Changed` covers field drift and source-absent objects; `SourceAbsent` requires a successful accepted-source lookup.
Bind playbooks through findings, to the faulty component only: for a journey failure embed `localize.Inputs` and
return `localize.BindSuspects` of `localize.FromContext`, not a literal name.
The shared view adds previous-instance logs, usage, probes, baseline diffs, metric queries, alerts and traffic.
The runtime caches reads per snapshot and records results and failures for replay.
Logs cap at 64 KiB/5 s, probes at 5 s. Read failures are errors/Unknown, never clearance;
a successful probe with `Success:false` is an observed failure. Keep always-on reads cheap and use Diagnose for depth.
To judge state only a running process knows (a store's config, role or access list), even before users are affected,
declare [pod reads](references/pod-reads.md).
Test using `sdktest.Snapshot.Readings` plus phase recordings; never fill absent evidence with healthy answers.

## Synthetic traffic confirmation

Steady traffic remains pending until every scenario has succeeded since observation began and has enough recent usable SLO samples. Pending carries a reason naming unproven scenarios, establishes no health and never opens an incident by itself. Qualified violations still fire alongside unproven peers. Process restarts lose qualification; recovery requires fresh observations and the controller's ordinary clearing check. A verify burst reports present scenario evidence, while controller verification owns incident closure.

Health traffic sees only what it calls: give every `links.yaml` edge a continuous scenario that crosses it, writes
included, bound each scenario's latency and cover every seeded record; see [traffic coverage](references/traffic-coverage.md).

## Logs and usage detectors

A log detector implements `logs.Consumer`: `LogQueries()` returns its queries (an ID, RE2
patterns with their own IDs, and `Previous` for a crashed container's tail). Declare `logs.Watch`, read with
`logs.Require(ctx, queryID)`, and decide with `logs.Appeared`, `logs.Rate` or `logs.Silent`, which take a pattern
ID. A kubelet usage detector implements `usage.Consumer`: `UsageRequirements()` returns
`usage.Requirements{Throttling: true}` only when it needs CPU throttling. Declare `usage.Watch` and read with
`usage.From(ctx)`. A missing input or a source error is a detector error, never a clear. Test with
`sdktest.Snapshot.Logs` and `sdktest.Snapshot.Usage`.

## Metrics detectors (optional input)

When the controller runs with a bound telemetry source, a health detector can
judge application metrics through `sdo.dev/operator/sdk/metrics`. Declare `metrics.Watch`
and an `Interval`; build the detector with `metrics.NewThresholdDetector(spec, rules)`. A rule is a query
(`metrics.Query{ID, Expr}`, plus `Range`/`Step` for a range query), a comparison, a threshold, and the label names that
map a series to a Deployment, Pod or Service. Ground each expression in metrics the application exports; never assume
a source exists.

- The runtime freezes each declared query's result into the snapshot once per cycle. The detector does no I/O; read results with `metrics.ResultFrom` and test with `sdktest.Snapshot.Metrics`.
- An unreachable source, a failed query or a missing result is a detector error, never a clear and never a finding.
- Findings carry the query, value, threshold and labels as evidence, so each series is a distinct finding.
- `Query.Source` (optional): [metrics sources](references/metrics-sources.md).
- `memory.PrepareHealth` installs the generic `metrics-alerts` detector (`metrics.NewAlertsDetector`: firing alerts of
  the application namespace, one finding each) only when a bound source serves alerts; do not author it.

## Generic reachability baseline

Service reachability needs five consecutive 1s failures, and exclusions are measured in cluster state, never `.sdo/`: [reachability baseline](references/reachability-baseline.md).

For kinds outside the typed getters, use the [generic object interface](references/generic-objects.md).
