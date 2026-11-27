# Probe design

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
| `... onboarding journeys --alter --soak 5m --only ID` | in the background, after fixing a scenario that failed while healthy |

Reports put red lines first. Write long output to a file and read the red lines; never poll: run a long check in the
background and write the next probe meanwhile.
