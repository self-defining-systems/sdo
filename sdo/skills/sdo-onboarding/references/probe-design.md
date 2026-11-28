# Probe design

- A journey is what a user does: write something with a fresh unique name, then read it back exactly through the
  route a user reads, including the background effect (wrap that read in `traffic.Eventually`, or return `NotYet`). Read journeys check exact content of
  fixtures Setup made, and one step lists records users already have. Each invariant checked on fixtures
  is also checked, read-only, on a sample of users' records: a fault in users' data never touches fixtures.
- Write values the size and shape users write. The goal's `Test writes:` line says whose data probes act on:
  `own` (the default) means probe accounts and probe records only; `shared` also allows users' own records and
  the accounts the goal names, so a fault that hits only users' data is seen.
- Cover each way in (API, other protocols, sign-in) and each store or component on its path, refused access as well
  as granted access where the journey is about permissions.
- Several journeys fit in one scenario only when they share data; a scenario has at most 8 steps. Prefer one request
  that checks more over two.
- Two probers may run it at once, beside real users: name every object per iteration, look up by id or marker,
  never assume a list is yours or a fixture is the only one. Writers that contend share one identity per journey.
- Clean up in the same iteration, in an order the application allows while its background work runs. SDO reads a
  DELETEd path back itself; any other cleanup ends with a read that shows the data gone (404, or absent from the list). A delete answered 2xx may only schedule
  the removal (delayed deletion, trash, soft delete): use the application's remove-for-good path, and accept only
  success or not-found, never a refusal. Data that cannot be deleted stays one fixed object (`idempotent-write`, no `Unique`), or is bounded and noted.
- Probe data left behind must never change what users see. The read of records users already have lists the way a user first
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
- Leave `maxLatency` out: after install SDO sets it from the soak to three times the slowest healthy run, at least 1 s.

## Checks

| Command | When |
|---|---|
| `sdo-operator onboarding journeys --repo DIR --namespace NS --plan` | first, and after each map or workload edit (no cluster) |
| `sdo-operator onboarding journeys --repo DIR --namespace NS --only ID --quick` | after each edit of one probe: one iteration, each request and answer printed |
| `--only ID`, with `--alter` for an altered-answers red | in the follow-up, to re-check a scenario SDO named red |
| `sdo-operator onboarding check --metrics --repo DIR --namespace NS` | once, when a request counter is mapped |

Every run but `--quick` also runs `memory check` (red `memory-check` lines), so it needs no separate call. SDO runs
every probe's repeatability, concurrency, cleanup and altered-answer checks after the session.

## SDK details

- Paths, `WithJSON` bodies, `JSONEquals` and `ExactSet` fill `{key}` from State (query and form values take
  `FromPrevious`); `Contains` and `Each` use their text and pattern as written.
- State holds only what `SaveJSON`, `SaveCookie` and `FromSeedPair` stored this iteration. A generated value
  (`Unique`) is not kept: save it from the answer to use it in a later step.
- `Eventually(read)` sends a GET again while its checks fail with an answer below 500, until they pass or the
  `iterationTimeout` nears; a custom `Check` returns `NotYet` for the same.
- `ReadsExistingRecords` passes on an empty list; a member that contains the scenario's Marker is the probe's own.
- Every write of a write scenario carries its Marker (path, body, query or a header; a parameter the application
  ignores is fine), or names a record a marked write saved.
