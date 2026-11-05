---
name: sdo-operationalize
description: Detection design for an application. Map its operations, components and call edges, write probes and detectors from the map, and calibrate them on the live application.
---

# Detection design

The detection designer (task actor `health_judge`) decides what SDO watches. It works in three steps: map the
application, write probes and detectors from the map, and calibrate them on the live application. Keep the
result small and general: the map and the three probe kinds should catch whatever breaks an operation, a hop
or stored data. Write probes with the `sdo-detector-authoring` traffic reference; use the `sdo-memory` commands.

## 1. Map the application

Read the source, its config and the manifests (the Go inventory in the architecture text lists every manifest
resource). Then look at the live application. Write the map in the format of [map](references/map.md), between
the map markers in the architecture file. Only the text between the markers is yours.

- **Operations**: every user-facing route at the entry point (router, mux, proxy config), with its Service,
  port, method and path. `writes: true` when it changes stored state.
- **Components**: one per application workload, with the Services that select it. `holds_data: true` when it is
  the only place some user-visible data lives: losing its contents loses data. A cache that refills from
  another component on a miss does not hold data.
- **Edges**: caller to callee. `config` evidence cites the source file that names the callee (a client address,
  an env value, a config entry). `observed` evidence says how you saw the call on the live application.
- **Paths**: for each operation, follow its handler through the code and the edges, and list every component
  it reaches, entry component first.

## 2. Write probes and detectors

Three probe kinds, each a set of traffic scenarios in its own continuous health workload:

| Kind | Workload | One scenario per | It checks |
|---|---|---|---|
| call | `probe-call` | read operation | status, latency, and body content the healthy application returns |
| write-read | `probe-write` | writing operation | the write succeeds and a read operation then returns the written value |
| read-later | `probe-read-later` | data-holding component on a write path | a fixture written earlier is still returned |

- Go installs one generic persistent-failure detector per health workload (`traffic-<workload>`). Do not write
  detector code for these probes. Do not lower their persistence.
- Every scenario's `DependsOn` lists the Services of every component on its map path, and nothing else. A
  failing probe then names its suspects.
- Add a `probes` entry to the map for every scenario: its kind and the operations its steps exercise.
- Writes use a dedicated test identity (a synthetic user, account or key whose name contains the scenario's
  `Marker`), never real users' data. Put the marker in every write request. Make the read-back check a value
  the write chose (`SaveJSON`/`FromPrevious`, or a unique marked value from the rng).
- Clean up what a write creates when the application has a delete or overwrite route
  (`SideEffectWriteWithCleanup`). Without one, keep every write on the fixed test identity, declare
  `SideEffectIdempotentWrite`, use the lowest rate that fills the window, and record the growth under
  `Probe costs` in the health-objective playbook.
- A read-later fixture is data the application's own deployment seeds when there is any. Otherwise, write the
  fixture once, during calibration, through the application's own write operation, on the test identity and
  with the marker. The read-later scenario is a read: it must find the fixture's value. Record the exact
  requests that re-create the fixture under `Fixtures` in the health-objective playbook.
- Keep the link workload for every map edge between Services (traffic authoring rules).
- Rates: each scenario needs at least the SLO's `minSamples` within `maxAge` (defaults 3 in 30s, so at least
  0.1 iterations per second per scenario). Keep all probe requests together under 5% of the application's
  normal request rate when you can measure it, and under 2 requests per second otherwise.

## 3. Calibrate on the live application

From your session, reach the live application through its Services (cluster DNS, or `kubectl` when available).

1. Send each scenario's requests once by hand. Each must pass on the healthy application. Write the read-later
   fixtures now and read them back.
2. Check the observed path: for each probe, confirm that every component on its map path served it, using what
   the application offers (trace spans, access logs, or connection and request counters on each pod). Add an
   observed edge you did not map; recheck the source for a mapped hop you never saw.
3. Set each scenario's `maxLatency` from its measured healthy latency (with headroom), or from a client timeout
   the source states.
4. Compute the probe request rate (iterations per second times requests per iteration, summed over the three
   workloads) and compare it with the application's request rate if you can observe it.
5. Write a `Calibration` section in the health-objective playbook: what passed, the observed paths, the probe
   rate and anything you could not check (no live access counts; say so). Then run `sdo-operator memory check`
   until it exits 0.

## Other detectors

Go already installs generic checks of workloads, Services and nodes. Add a source-specific cause detector only
for a mechanism the probes and generic checks cannot localize ([catalog](references/catalog.md),
[methods](references/methods.md)). Never weaken a probe to silence it; fix the scenario or the threshold.

## After an incident (reflection)

- A fault that only a probe caught, with no cause check naming it: add an incident detector for its mechanism,
  never one the Go-installed checks already cover.
- A detector that fired while nothing was wrong: tighten it with the outcome's evidence; it must still fire on
  its captured cases and stay quiet on healthy baselines.
- Responders own incident detectors and the detection designer owns health detectors and the map.

## Application instrumentation

When a health objective or incident cannot be localized with existing inputs, add instrumentation to the
application's own source through the normal source-repair path. Export request and error counters, latency
histograms and queue depth; keep labels bounded. Reflection records the need for a source repair rather than
editing source itself. Use the task's typed `metrics_source`: annotate pod templates with
`prometheus.io/scrape: "true"`, `prometheus.io/port` and `prometheus.io/path` when annotated scraping is enabled,
or publish OTLP HTTP metrics to its full `otlp_metrics_url`. Do not infer publishing support from a query URL.
Write source-grounded PromQL rules with `sdo.dev/operator/sdk/metrics`, declare `metrics.Watch`, and use
`metrics.NewThresholdDetector`; the controller queries and freezes results, and detectors never perform I/O.
