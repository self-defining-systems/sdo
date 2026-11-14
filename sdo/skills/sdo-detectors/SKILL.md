---
name: sdo-detectors
description: Write SDO's probes and detectors and prove them. Covers the three probe kinds, strict probes, metric and cause detectors, the Go SDK, tests, calibration and the memory check.
---

# Probes and detectors

SDO watches an application with probes (synthetic user requests that check results) and detectors
(deterministic Go over the runtime snapshot). Real traffic shows most failures
([observability](../sdo-observability/SKILL.md)); probes cover what traffic cannot show: wrong answers, quiet
operations, existing records and background work. A probe must pass when healthy and fail when its operation
is broken. Keep the set small and general.

## Probes

Three kinds, each a set of traffic scenarios in its own continuous health workload (write a kind's workload
only if it has a scenario):

| Kind | Workload | One scenario per | It checks |
|---|---|---|---|
| call | `probe-call` | read operation | status, latency and the exact content the healthy application returns |
| write-read | `probe-write` | writing operation | a change the iteration just made, read back exactly |
| read-later | `probe-read-later` | data-holding component on a write path | a fixture written once is still returned exactly |

- Go installs one generic persistent-failure detector per health workload (`traffic-<workload>`). Do not write
  detector code for these probes, and do not lower their persistence.
- Each scenario's `DependsOn` lists the Services of every component on its map path, and nothing else. Add a
  `probes` entry to the map for it ([map](../sdo-operations-map/SKILL.md)).
- Check exact content, never only statuses ([strict probes](references/probes.md)), against the operation's
  `correct` note. Read back exactly the change each write-read iteration just made.
- Writes use a synthetic test identity named with the scenario's `Marker`, never real users' data. Never forge
  credentials from application secrets: sign in as an identity `Setup` created, or use a
  [probe identity](../sdo-operations-map/references/map.md#probe-identity).
- Probes are self-contained: each passes on a freshly deployed application with nothing set up by hand.
  `Setup` steps are ensure steps (`OrExisting(status, text)` accepts the duplicate answer).
- Read the records users already have, per health objective (`ReadsExistingRecords`), else declare
  `no_user_records: <why>` ([Users' own records](references/probes.md#users-own-records)).
- Clean up what a write creates when a delete route exists (`SideEffectWriteWithCleanup`); else bound its growth.
- Every scenario is sampled at least every 10 s; keep probe requests under 5% of the app's request rate, else
  under 3 per second. Signatures and options: [traffic reference](references/traffic.md).

## Prove each probe

1. Send each scenario's setup and steps by hand on a healthy sandbox; send each setup twice.
2. Confirm every component on its map path served it (traces, access logs, per-pod counters).
3. Break its operation: `sdo perturb scale-zero COMPONENT --on sandbox-N` for each non-store component on the
   path. The probe must fail and raise within 60 s, and clear after the undo. A probe that stays green checks
   too little.
4. Detect by correctness first; a latency bound is a backstop. Time whole scenarios over at least 20 healthy
   iterations and set `maxLatency` to the larger of three times the slowest and 1 s. Record fastest, median,
   slowest and the bound in a `Calibration` section of the health-objective playbook.

## Detectors

- Go installs generic checks of workloads, Services, nodes, mapped edges and data stores. Do not author copies.
- Metric detectors judge the app's own series (`sdo/operator/sdk/metrics`); write one only for a failure the
  per-operation series and probes cannot show.
- [Data checks](references/data-checks.md) cover derived stores and records the read path decodes. A cause
  detector covers a mechanism nothing else localizes ([catalog](references/cause-catalog.md),
  [methods](references/methods.md)). Never weaken a probe to silence it.
- Detector code: [writing detectors](references/detector-code.md), the [generated SDK](references/sdk.md), the
  [validator rules](references/rules.md). It is deterministic Go: no model, network, clock or environment.
- Test with `sdktest` snapshots: a matching case and a near miss for each detector.

## Check before you commit

Run `sdo-operator memory check` until it exits 0; `sdo-operator memory test <id>` runs one detector's tests.
The broker runs the same check on every commit.

## After an incident (responders)

- A fault only a probe caught, with no cause check naming it: add an incident detector for its mechanism.
- A detector that fired while nothing was wrong: tighten it with the outcome's evidence.
- Responders own incident detectors; health detectors and the map belong to onboarding.

<!-- reference-topics -->
## Reference topics

Open only the topic needed for the current edit.

- [app-state](references/app-state.md)
- [cause-specific](references/cause-specific.md)
- [generic-objects](references/generic-objects.md)
- [pod-reads](references/pod-reads.md)
- [reachability-baseline](references/reachability-baseline.md)
- [sdktest-01](references/replay/sdktest-01.md)
- [sdktest-02](references/replay/sdktest-02.md)
- [sdktest-03](references/replay/sdktest-03.md)
- [replay](references/replay.md)
- [traffic-coverage](references/traffic-coverage.md)
