---
name: sdo-onboarding
description: Onboard a deployed application as SDO's onboarding agent. Covers turning the given user journeys into probes, the map, request metrics, the request budget, the checks and the finish line.
---

# Onboarding an application

The application runs; the people who run it listed its user journeys. You write what SDO needs to notice a broken
journey within seconds: the map (`.sdo/arch.md`), probes (`.sdo/diagnostics/traffic/`) and the health-objective
playbook. Go adds the generic Kubernetes, endpoint, store and request-metric detectors when memory is published;
do not write them. Work alone: one author, no subagents, no reviewers.

## Order of work

1. **Look once** (a few minutes): manifests, README, Services, the API's own description, the way a user gets an
   account. Note what each journey calls and what a correct answer is.
2. **Map**: start from Go's seed, then entries, operations with `journey: <id>` and `correct`, probes
   ([map](../sdo-operations-map/SKILL.md)). Declare the [probe identity](../sdo-operations-map/references/map.md#probe-identity)
   when accounts are not self-service: ordinary users, never admin.
3. **One probe first**, end to end through `onboarding journeys --only`, then the rest in batches of three or four.
   Copy the shape of the [worked example](references/example.md); read the [traffic reference](../sdo-detectors/references/traffic.md)
   only for a signature it does not show, and [strict probes](../sdo-detectors/references/probes.md) once.
4. **Request metrics**: map the request metric the application already exposes (route, method, status). When its
   route label does not tell operations apart, fix that by configuration or a small middleware in the deployment;
   time-box it, since probes carry detection ([observability](../sdo-observability/SKILL.md)).
5. **Check, fix the first red line, repeat.** Commit when a batch is green.

## What a good probe is

- A journey is what a user does: write something with a fresh unique name, then read it back exactly through the
  route a user reads, including the background effect (poll it with `NotYet`). Read journeys check exact content of
  fixtures Setup made, and one step lists records users already have.
- Cover each way in (API, other protocols, sign-in) and each store or component on its path, refused access as well
  as granted access where the journey is about permissions.
- Several journeys fit in one scenario only when they share data; a scenario has at most 8 steps. Prefer one request
  that checks more over two.
- Two probers may run it at once, beside real users: name every object per iteration, look up by id or marker,
  never assume a list is yours or a fixture is the only one. Writers that contend share one identity per journey.
- Clean up in the same iteration, in an order the application allows while its background work runs. Data that
  cannot be deleted stays one fixed object, or is bounded and noted.
- Keep probe work small: few concurrent heavy writes, small objects, low-cost reads.

## Budget and cadence

`onboarding journeys` prints requests/s planned and left under the cap: look before you add. Set `ratePerSecond` in
every workload (unset is 4 iterations/s). Give most of the budget to the journeys users do most:

- `probe-call`, `probe-write`, `probe-read-later` hold every scenario of their kind. Give a home workload a long
  `sampleInterval` (60-120 s) with `minSamples: 1, maxErrorRate: 1, maxTimeoutRate: 1, maxAge: 10m`: SDO confirms a new failure itself
  with two more samples within 5 s.
- Add `probe-fast-call` and `probe-fast-write` with the core scenarios again at 10 s, so the page comes within about 20 s. Cover
  each store and each component a user request crosses from a fast workload; rarer journeys stay in the home workload.
- `maxLatency` about three times the slowest healthy run, at least 1 s.

## Checks

| Command | When |
|---|---|
| `sdo-operator onboarding journeys --repo DIR --namespace NS --only ID` | after each probe (seconds once built) |
| same with `--alter` | when it passes: an unnoticed wrong answer is a missing check |
| `sdo-operator memory check` | after each batch; map, coverage, ownership, compile |
| `sdo-operator onboarding check --metrics --repo DIR --namespace NS --prometheus URL` | once, when metrics are mapped |
| `... onboarding journeys --alter --soak 5m` | last |

Reports put red lines first. Write long output to a file and read the red lines; never poll: run a long check in the
background and write the next probe meanwhile.

## Finish line

Every given journey has an operation and a probe, `onboarding journeys --alter --soak 5m` and `memory check` are
green on the committed tree, and the application is as healthy as you found it. With ten minutes left, stop adding:
remove what is not green, commit, and say what is uncovered.
