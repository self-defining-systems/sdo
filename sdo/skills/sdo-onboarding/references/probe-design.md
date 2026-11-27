# Probe design

- A journey is what a user does: write something with a fresh unique name, then read it back exactly through the
  route a user reads, including the background effect (wrap that read in `traffic.Eventually`, or return `NotYet`). Read journeys check exact content of
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
  success or not-found, never a refusal. Data that cannot be deleted stays one fixed object (`idempotent-write`, no `Unique`), or is bounded and noted.
- Probe data must never change what users see. The read of records users already have lists the way a user first
  sees them (default order, first page, no filter that hides other people's records) by a member that shows names,
  so probe records left behind show up there and the healthy run turns red.
- Keep probe work small: few concurrent heavy writes, small objects, low-cost reads.

## Budget and cadence

Omit `sampleInterval` and `ratePerSecond` to use the 30 s detection target. The human goal can override it
with a line such as `Detection target: 45s`. Go reserves time for confirmation and samples each scenario
once per remaining interval, within 10–120 s. Explicit workload fields override their defaults.
Check the worst detection time and the requests/s cap with `onboarding journeys --plan`.
Give each scenario one home workload. Spend remaining budget on the most used journeys, adding faster workloads
for core paths when their planned time is too long.
- Set `maxLatency` to about three times the slowest healthy run, at least 1 s. A run over budget starts confirmation.

## Checks

| Command | When |
|---|---|
| `sdo-operator onboarding journeys --repo DIR --namespace NS --plan` | first, and after each map or workload edit (no cluster) |
| `sdo-operator onboarding journeys --repo DIR --namespace NS --only ID` | while building the first batch |
| same with `--quick` | edit loop once memory validates: run memory check, then one iteration with a step trace |
| same with `--alter` in place of `--quick` | after it passes: check repeatability, concurrency and wrong answers |
| `sdo-operator onboarding check --metrics --repo DIR --namespace NS` | once, when a request counter is mapped |
| `... onboarding journeys --alter --soak 5m --only ID` | in the background, after fixing a scenario that failed while healthy; stops once each scenario is decided |

Every run also runs `memory check` (red `memory-check` lines), so it needs no separate call. Reports give one line per scenario with details for failures. Use `--verbose` for every check. Run long checks in
the background and write the next probe meanwhile.
