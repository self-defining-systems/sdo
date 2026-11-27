---
name: sdo-onboarding
description: Onboard a deployed application as SDO's onboarding agent. Covers turning the given user journeys into probes, the map, request metrics, the request budget, the checks and the finish line.
---

# Onboarding an application

Work alone. Author the application map, traffic probes (`.sdo/diagnostics/traffic/`) and health-objective playbook
to detect broken journeys within seconds. Go adds generic detectors at publication. Preserve `.sdo/goal.md`.
Commit only files the initial health judge owns. Edit `.sdo/arch.md` inside its application-map markers.
Go owns the remaining architecture, schema, outcomes and lifecycle provenance.

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
   using the example. Use `probe_identity` for ordinary accounts that require admin approval. For a token API,
   use the example's provisioned-token option.
3. **One probe first**, end to end through `onboarding journeys --only`, then the rest in batches of three or four.
   Start with the [worked example](references/example.md). Open the [map](../sdo-operations-map/references/map.md)
   or [traffic](../sdo-detectors/references/traffic.md) reference only for a missing field or signature.
4. **Request metrics (optional)**: when the app already exposes a request counter by route, method and status,
   map it and confirm in `onboarding check --metrics` that the mapped series distinguish the operations. Without
   `--prometheus` the check runs SDO's own Prometheus. When the app has no such counter, probes alone are enough:
   note the reason under `## Request metrics` and go on. Do not change application code or deploy a backend for it.
5. **Check, fix the first red line, repeat.** Commit when a batch is green.

## What a good probe is

- A journey is what a user does: write something with a fresh unique name, then read it back exactly through the
  route a user reads, including the background effect (poll it with `NotYet`). Read journeys check exact content of
  fixtures Setup made, and one step lists records users already have.
- Write values the size and shape users write, where users write them.
- Cover each way in (API, other protocols, sign-in) and each store or component on its path, refused access as well
  as granted access where the journey is about permissions.
- Several journeys fit in one scenario only when they share data; a scenario has at most 8 steps. Prefer one request
  that checks more over two.
- Two probers may run it at once, beside real users: name every object per iteration, look up by id or marker,
  never assume a list is yours or a fixture is the only one. Writers that contend share one identity per journey.
- Clean up in the same iteration, in an order the application allows while its background work runs, and end the
  cleanup with a read that shows the data gone (404, or absent from the list). A delete answered 2xx may only schedule
  the removal (delayed deletion, trash, soft delete): use the application's remove-for-good path, and accept only
  success or not-found, never a refusal. Data that cannot be deleted stays one fixed object, or is bounded and noted.
- Probe data must never change what users see. The read of records users already have lists the way a user first
  sees them (default order, first page, no filter that hides other people's records) by a member that shows names,
  so probe records left behind show up there and the healthy run turns red.
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
- Set `maxLatency` to about three times the slowest healthy run, at least 1 s. A run over budget starts confirmation.

## Checks

| Command | When |
|---|---|
| `sdo-operator onboarding journeys --repo DIR --namespace NS --plan` | first, and after each map or workload edit (no cluster) |
| `sdo-operator onboarding journeys --repo DIR --namespace NS --only ID` | after each probe (seconds once built) |
| same with `--alter` | when it passes: an unnoticed wrong answer is a missing check |
| `sdo-operator memory check` | after each batch; map, coverage, ownership, compile |
| `sdo-operator onboarding check --metrics --repo DIR --namespace NS` | once, when a request counter is mapped |
| `... onboarding journeys --alter --soak 5m` | initial scheduled run |
| same with `--only ID` | after fixing a scenario or a dependency it uses |

Reports put red lines first. Write long output to a file and read the red lines; never poll: run a long check in the
background and write the next probe meanwhile.

## Finish line

Every journey passes and every scenario has a green healthy-run line. Re-soak only scenarios affected by a fix.
Confirm mapped per-route counters and document unavailable metrics, traces or logs. Missing telemetry never blocks
onboarding. As an ordinary user, confirm default list pages show no probe records.
With ten minutes left, stop adding. Remove work that is still red and say what is uncovered. Commit all export edits
and pass `sdo-operator memory check` on the committed tree. If the session rejects an export, fix the diagnostics
and commit again. SDO keeps the validated export even when coverage is partial.
