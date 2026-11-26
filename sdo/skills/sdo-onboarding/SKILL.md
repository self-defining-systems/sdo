---
name: sdo-onboarding
description: Onboard a deployed application as SDO's onboarding agent. Covers turning the given user journeys into probes, the map, request metrics, the request budget, the checks and the finish line.
---

# Onboarding an application

The application runs; the people who run it listed its user journeys. You write what SDO needs to notice a broken
journey within seconds: the map (`.sdo/arch.md`), probes (`.sdo/diagnostics/traffic/`) and the health-objective
playbook. Go adds the generic Kubernetes, endpoint, store and request-metric detectors when memory is published;
do not write them. Work alone: one author, no subagents, no reviewers.

## The journey checklist is your to-do list

`onboarding journeys` starts with one line per journey of `.sdo/goal.md`, in the goal's order. Run it with `--plan`
first: it needs no cluster and sends nothing. Work top to bottom, fixing the furthest-from-done state first:
`missing-map` (map an operation with `journey: <id>`), then `missing-probe` (a scheduled probe; a write needs a
read-back), then `failing`, then `unchecked` (judge it with `--only`). Every journey must reach `passed`. Never drop or
rename a goal journey. A journey you cannot cover stays on the list, and your final line names it.

Each line says how soon a full failure of the journey is noticed (about / at worst). This is computed from your
workloads' schedule and SDO's confirmation, not measured. It also names the operation and probe that bound it and what
sampling that probe at the 10 s floor would cost. Use it to spend the budget: make the earlier and more used journeys
fast first, and stop when the budget line shows little left.

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

- `probe-call`, `probe-write`, `probe-read-later` hold every scenario of their kind, once: a write journey needs no
  `call` twin. A home workload takes a longer `sampleInterval` with
  `minSamples: 1, maxErrorRate: 1, maxTimeoutRate: 1, maxAge: 10m`: SDO confirms a new failure itself with two more
  samples within 5 s, so a failure is noticed about one interval after it starts.
- Add `probe-fast-call` and `probe-fast-write` naming the same core scenario IDs again at 10-15 s, so that every store,
  worker and protocol is crossed by one of them. Spend the rest on the home workloads' interval: the budget line is
  red while a slow workload leaves budget unspent.
- `maxLatency` about three times the slowest healthy run, at least 1 s.

## Checks

| Command | When |
|---|---|
| `sdo-operator onboarding journeys --repo DIR --namespace NS --plan` | first, and after each map or workload edit (no cluster) |
| `sdo-operator onboarding journeys --repo DIR --namespace NS --only ID` | after each probe (seconds once built) |
| same with `--alter` | when it passes: an unnoticed wrong answer is a missing check |
| `sdo-operator memory check` | after each batch; map, coverage, ownership, compile |
| `sdo-operator onboarding check --metrics --repo DIR --namespace NS --prometheus URL` | once, when metrics are mapped |
| `... onboarding journeys --alter --soak 5m` | last |

Reports put red lines first. Write long output to a file and read the red lines; never poll: run a long check in the
background and write the next probe meanwhile.

## Finish line

Every journey on the checklist is `passed`, `onboarding journeys --alter --soak 5m` and `memory check` are
green on the committed tree, and the application is as healthy as you found it. With ten minutes left, stop adding:
remove what is not green, commit, and say what is uncovered.
