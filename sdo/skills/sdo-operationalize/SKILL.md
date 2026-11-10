---
name: sdo-operationalize
description: Detection design for an application. Map its operations, components and call edges, write probes and detectors from the map, and calibrate them on the live application.
---

# Detection design

The detection designer (task actor `health_judge`) decides what SDO watches. It works in three steps: map the
application, write probes and detectors from the map, and calibrate them on the live application. Keep the
result small and general: the map and the three probe kinds should catch whatever breaks an operation, a hop
or stored data. Write probes with the `sdo-detector-authoring` traffic reference; use the `sdo-memory` commands.
The memory check enforces the map [rules](references/rules.md). `operability.*` findings: [feedback](references/feedback.md).

## 1. Map the application

Read the source, its config and the manifests (the Go inventory in the architecture text lists every manifest
resource). Then look at the live application. Write the map in the format of [map](references/map.md), between
the map markers in the architecture file. Only the text between the markers is yours.

- **Operations**: every user-facing route at the entry point (router, mux, proxy config), with its Service,
  port, method and path. `writes: true` when it changes stored state.
- **Components**: one per application workload, with the Services that select it. `holds_data: true` when it is
  the only place some user-visible data lives. A cache that refills on a miss does not hold data. SDO never lets
  a learned program restart, delete or scale down a `holds_data` workload automatically. Every store gets a
  `data` entry.
- **Edges**: caller to callee. `config` evidence cites the source file that names the callee (a client address,
  an env value, a config entry). `observed` evidence says how you saw the call on the live application.
- **Paths**: for each operation, follow its handler through the code and the edges, and list every component
  it reaches, entry component first.

## 2. Write probes and detectors

Three probe kinds, each a set of traffic scenarios in its own continuous health workload (write a kind's workload only if it has a scenario; omit its file and map rows otherwise):

| Kind | Workload | One scenario per | It checks |
|---|---|---|---|
| call | `probe-call` | read operation | status, latency, and the exact content the healthy application returns |
| write-read | `probe-write` | writing operation | a change the iteration just made, read back exactly |
| read-later | `probe-read-later` | data-holding component on a write path | a fixture its setup wrote once is still returned exactly |

- Go installs one generic persistent-failure detector per health workload (`traffic-<workload>`). Do not write
  detector code for these probes. Do not lower their persistence.
- Every scenario's `DependsOn` lists the Services of every component on its map path, and nothing else.
- Add a `probes` entry to the map for every scenario: its kind and the operations its steps exercise.
- Writes use a dedicated test identity (a synthetic user, account or key whose name contains the scenario's
  `Marker`), never real users' data. Put the marker in every write request. Never forge credentials or tokens
  from application secrets or config: log in as a test identity Setup created.
- Check exact content, never only statuses, and read back exactly the change each write-read iteration just
  made ([strict probes](references/probes.md)): re-applying one write every iteration re-heals what it reads.
- Probes are self-contained: each passes on a freshly deployed application with nothing set up by hand. A
  scenario creates its test identities and data in `Setup` steps, through the application's write operations,
  with the marker. Each setup step is an ensure step: `OrExisting(status, text)` accepts the application's
  duplicate answer (from the handler's source, confirmed live). Setup runs once per prober start and verify
  bursts reuse it, so data lost later fails the steps (bursts too) until it comes back; only a controller
  restart writes it again. Steps read only data a setup or earlier step wrote, or records the deployment itself
  seeds (confirmed live).
- Clean up what a write creates when the application has a delete or overwrite route
  (`SideEffectWriteWithCleanup`); otherwise bound and record its growth ([strict probes](references/probes.md#cost)).
- A read-later scenario is a read (`SideEffectRead`) with a `Marker`: its `Setup` writes a fixed marked
  fixture, on an identity no other scenario writes, through the writing operation whose data it checks; its
  steps read it back. List setup's identities and fixtures under `Fixtures` in the health-objective playbook.
- Keep the link workload for every map edge between Services (traffic authoring rules).
- Rates: give every scenario (workload rate times its weight share) at least 0.2 iterations per second, twice
  the SLO default of 3 samples in 30s, or health is never confirmed; chain operations rather than lower rates.
  Keep all probe requests under 5% of the app's request rate when measurable, else under 3 per second.

## 3. Calibrate on the live application

Always calibrate, also when a map and probes already exist: an earlier session's bounds and rates can be wrong.
Reach the live application through its Services (cluster DNS, or `kubectl` when available).

1. Send each scenario's setup and step requests by hand. Each must pass on the healthy application. Send every
   setup request twice: the second answer must match its `OrExisting` status and text.
2. Check the observed path: for each probe, confirm that every component on its map path served it, using what
   the application offers (trace spans, access logs, or connection and request counters on each pod). Add an
   observed edge you did not map; recheck the source for a mapped hop you never saw.
3. Detect by correctness first (status, body, read-back); a latency bound is a backstop that must never page
   on a busy healthy host. Time each whole scenario (all steps, no setup) over at least 20 healthy iterations
   at the workload's rate, never single requests. Set `maxLatency` to the larger of three times the slowest
   and 1 s, below the iteration timeout. Record fastest, median, slowest and the bound in `Calibration`.
4. Compute the probe request rate (iterations per second times requests per iteration, summed over the three
   workloads) and compare it with the application's request rate if you can observe it.
5. Write a `Calibration` section in the health-objective playbook: what passed, the observed paths, the probe
   rate and anything you could not check (no live access counts; say so). Then run `sdo-operator memory check`
   until it exits 0.

## Other detectors

Go already installs generic checks of workloads, Services and nodes. Add a source-specific cause detector only
for a mechanism the probes and generic checks cannot localize ([catalog](references/catalog.md),
[methods](references/methods.md)). Never weaken a probe to silence it; fix the scenario or the threshold. When
existing inputs cannot localize a failure, instrument the application's source through the source-repair path
([application instrumentation](references/methods.md#application-instrumentation), [telemetry](references/telemetry.md)).

## After an incident (reflection)

- A fault that only a probe caught, with no cause check naming it: add an incident detector for its mechanism,
  never one the Go-installed checks already cover.
- A detector that fired while nothing was wrong: tighten it with the outcome's evidence (fire on captured cases).
- Responders own incident detectors and the detection designer owns health detectors and the map.
