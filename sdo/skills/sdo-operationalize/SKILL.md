---
name: sdo-operationalize
description: Detection design for an application. Map its operations, components and call edges, write probes and detectors from the map, and calibrate them on the live application.
---

# Detection design

The detection designer (task actor `health_judge`) decides what SDO watches. It works in three steps: map the
application, write probes and detectors from the map, and calibrate them on the live application. Keep the
result small and general: the map and the three probe kinds should catch whatever breaks an operation, a hop
or stored data. Write probes with the `sdo-detector-authoring` traffic reference; use the `sdo-memory` commands.
Start from [real traffic](references/real-traffic.md): name each operation's failure series.
The memory check enforces the map [rules](references/rules.md). `operability.*` findings: [feedback](references/feedback.md).

## 1. Map the application

Read the source, config, manifests and the live application. Start from
Go's [seeded map](references/map.md#seeded-map) when no one wrote one. Write it in the [map](references/map.md)
format (`knownBroken` for a route broken as deployed) between the architecture file's map markers; only that text is yours. Record the ops [procedures](references/map.md#procedures) the app ships.

- **Operations**: every user journey: each entry point in source (routes, specs, proxy config, non-HTTP entries,
  workers, schedules) with its Service, port, method and path, or a recorded reason. `writes: true` when it
  changes stored state.
- **Components**: one per application workload, with the Services that select it. `holds_data: true` when it is
  the only place some user-visible data lives (a cache that refills on a miss does not). Every store gets a `data` entry.
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
- Writes use a synthetic test identity (user, account or key) named with the scenario's `Marker`, never real
  users' data, and carry the marker. Never forge credentials from application secrets or config: sign in as a
  test identity Setup created or, without self-service sign-up, a [probe identity](references/map.md#probe-identity).
- Cover what traffic cannot show ([probes](references/probes.md#what-traffic-cannot-show)): background work, rare operations, every protocol and direction.
- Read users' records: `ReadsExistingRecords` per health objective (empty passes), else `no_user_records: <why>` ([Users' own records](references/probes.md#users-own-records)).
- Check exact content, never only statuses, and read back exactly the change each write-read iteration just
  made ([strict probes](references/probes.md)): re-applying one write every iteration re-heals what it reads.
- Probes are self-contained: each passes on a freshly deployed application with nothing set up by hand. A
  scenario creates its test identities and data in `Setup` steps, through the application's write operations,
  with the marker. Each setup step is an ensure step: `OrExisting(status, text)` accepts the application's
  duplicate answer (from the handler's source, confirmed live). Each setup step runs once per prober start
  (retries skip passed steps); verify bursts reuse it, so lost data fails the steps until it comes back or a
  restart writes it again. Steps read data a setup or earlier step wrote, or records the deployment seeds
  (confirmed live).
- Clean up what a write creates when a delete or overwrite route exists (`SideEffectWriteWithCleanup`); else
  bound and record its growth ([strict probes](references/probes.md#cost)).
- A read-later scenario is a read (`SideEffectRead`) with a `Marker`: its `Setup` writes a fixed marked
  fixture, on an identity no other scenario writes, through the writing operation whose data it checks; its
  steps read it back. List setup's identities and fixtures under `Fixtures` in the health-objective playbook.
- Keep the link workload for every map edge between Services (traffic authoring rules).
- Rates: every scenario is sampled at least every 10 s whatever its weight (the prober raises the rate to
  `scenarios / 10` per second); weight shares the rest. Keep probe requests under 5% of the app's request rate,
  else under 3 per second (`_probe_load.over_cap` in the controller log).

## 3. Calibrate on the live application

Always calibrate, also when a map and probes exist: earlier bounds and rates can be wrong. Reach the live
application through its Services (DNS or `kubectl`).

1. Send each scenario's setup and step requests by hand. Each must pass on the healthy application. Send every
   setup request twice: the second answer must match its `OrExisting` status and text.
2. Check the observed path: for each probe, confirm that every component on its map path served it, using what
   the application offers (spans, access logs, per-pod counters). Add an
   observed edge you did not map; recheck the source for a mapped hop you never saw.
3. Detect by correctness first (status, body, read-back); a latency bound is a backstop that must never page
   on a busy healthy host. Time each whole scenario (all steps, no setup) over at least 20 healthy iterations
   at the workload's rate, never single requests. Set `maxLatency` to the larger of three times the slowest
   and 1 s, below the iteration timeout. Record fastest, median, slowest and the bound in `Calibration`.
4. Compute the probe request rate (iterations per second times requests per iteration, summed over the three
   workloads) and compare it with the application's request rate if observable.
5. Write a `Calibration` section in the health-objective playbook: what passed, the observed paths, the probe
   rate and anything you could not check (no live access counts). Run `sdo-operator memory check` until it exits 0.

## Other detectors

Go installs generic checks of workloads, Services and nodes. Write [data checks](references/data-checks.md)
for derived stores, keys resolved across stores and records the read path decodes. Add a cause detector only for
a mechanism nothing else localizes ([catalog](references/catalog.md), [methods](references/methods.md)); never
weaken a probe to silence it. Else [instrument](references/methods.md#application-instrumentation) the source.

## After an incident (reflection)

- A fault that only a probe caught, with no cause check naming it: add an incident detector for its mechanism,
  never one the Go-installed checks already cover.
- A detector that fired while nothing was wrong: tighten it with the outcome's evidence (fire on captured cases).
- Responders own incident detectors and the detection designer owns health detectors and the map.
