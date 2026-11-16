# The check report

`sdo-operator onboarding check` returns one report, tagged with the commit it checked:

```text
commit e4f1 · built 4 images (3 cached) · applied 37 · ready 2m41s
check: green 3 red 2
red journeys-move-own-metrics: 9 operations share route label value "unknown" (endpoint_id)
red tests-fail-broken: view-invoice probe passed with billing scaled to zero
```

The items are checks inside SDO's operability report (O1-O6), so the same result is what SDO records.

| Item | Green when |
|---|---|
| `deployed-healthy` | Patches apply, the render is deterministic, rollout is ready, objects belong to the app and stay in scope, memory is valid and every detector is clear. |
| `journeys-move-own-metrics` | SDO runs each operation's probes and counts what it sent. The operation's request series (built from its entry's metric and labels) carries its route value and rises by at least that count; its error series evaluates. Two operations with different routes have different route values. |
| `routes-match-operations` | Every exposed entry (Ingress, Gateway route, LoadBalancer, NodePort) has an `entries` record; its metric rises by at least what SDO sent there; every route value seen belongs to a declared operation. |
| `quiet-when-healthy` | No detector raises while the memory's detectors (test-request scenarios and the request-metrics detector included) are evaluated against the live application until every detector has judged 3 times (`--quiet-evaluations`) past its own clearing horizon, and none is still pending at the end. The window length is derived, not fixed: the check prints it when the first verdict arrives, and `--quiet-window` (10 minutes) is only the cap (a detector short of its judgements at the cap is red). The metric items run alongside the window. A raise names the detector, scenario and step. |
| `fails-under-break` | Run with `--break`, on a target without users. Each component on an operation's path is broken once (components that share no operation are broken together in one wave; the components behind the entries are first broken together in one pooled wave, and only those whose pooled break moved an operation or was not named by its own raise are broken again one wave at a time, so a component no test notices costs no extra wait; the verdicts are the ones separate breaks give, and a raise must still name the operation or component; raises a wave left clear before the next break or generic break starts) (by default denied ingress by a NetworkPolicy when the CNI enforces it, so the component keeps its endpoints and only failing requests show it down; otherwise scale-to-zero, and each line says which; `--break-mode scale` always scales) and restored. An operation is green only when a failure signal (within `--break-bound`, 60 s) names it: failing requests of the operation or a failing step of one of its probe scenarios. A generic detector that only names the broken component (for example `service-endpoints`) leaves the operation red: add or fix the test that exercises it. An unverifiable restore is red and stops the item. It runs even when `quiet-when-healthy` is red; raises standing before a break are ignored. A raise that only says the operation or its series are absent (`not instrumented`, no ready endpoints) is reported as `absence signal only` and leaves the operation red. |
| `operations-observed` | Every declared operation's route appears in its entry metric, and request-metrics does not report it as `not instrumented` on the healthy application (red naming the operation; no `reasons` entry excuses either; the line names the nearest observed label), and most of an entry's real traffic goes to declared routes, judged on the `probes-do-no-harm` paused window so the memory's own probes do not count. |
| `latency-per-operation` | A request-duration series carries each operation's route; red too when request-metrics reports the operation `not instrumented` while healthy. |
| `background-covered` | Every job or queue class in the app's job metrics is triggered by an operation (`triggers`) or excluded with a reason; otherwise it is reported as unattributed background work. A class an operation triggers must appear in the job metrics (`operations-observed`). |
| `probes-do-no-harm` | With the memory's probes running, no operation or job class fails more than with them paused, and the probes are not a large share of an entry's requests. The line names the failing operation or class and the probe scenarios; fix the probe so it leaves no objects the application's work then trips on. `--harm-window` (1 minute, 0 skips). |

| `probe-checks-result` | A probe lists the operation; one that writes data or triggers background work has a write-read or read-later probe that reads the effect; no healthy-window item found its scenarios failing. |
| `routes-assigned` | The map's `routes.list` command runs in its component's container and prints one `METHOD route` per line; each is a journey's route (`route`, else `entry.path`, with its method) or in `routes.excluded` with a reason. Red while the list is missing and per unassigned route; no `reasons` entry covers it. |
| `given-journeys` | When goal.md lists journeys: every listed journey has an operation naming it (`journey`), and no operation names an unlisted one. Each such operation is held to the table's checklist. |
| `map-pointers` | Every component on the operation's path has `observe` metrics and logs pointers. |
| `map-accuracy` | With `--break`, per broken component: the operations that failed (a failing request or probe step) match the operations whose path lists it. A missing edge (an operation failed but its path lacks the component) is red; a spurious one (listed, but nothing failed) is a note. A wave breaks components that share no operation, so a failure explained by any component in the wave is not a missing edge. |

With `--break`, `fails-under-break` also runs two breaks that keep every component up, each judged the same way (a raise within the bound must name an operation it hurts; every one is restored): `worker-quiet` (a worker stays Running and Ready but cannot reach anything except DNS; needs a CNI that enforces NetworkPolicy, otherwise a note) and `data-wrong` (a text column of the oldest rows of the most-read table of a postgres store is overwritten). A pod being up does not show them: a probe must read the result.

The report ends with a journey table, one row per declared operation and one column per item of the [journey standard](journey-standard.md): `a` mapping (`journeys-move-own-metrics`, `operations-observed`), `b` errors (`errors-separable-per-operation`, which also needs an error pattern that counts 5xx and not 4xx), `c` latency, `d` probe (`probe-checks-result`), `e` detection (`fails-under-break`, `quiet-when-healthy`), `f` map (`map-pointers`, `map-accuracy`), `g` traces (a note). Cells read `ok`, `note`, `RED`, or `n/r` (not run: `e` and `f` need `--break`). The report is clean only when every journey is complete on `a` to `f`.

Each live item and phase reports its duration (`time <item> <duration>`; the break item splits into judge, restore and settle), and a `timing:` line sums them up. Keep working while a check runs in the background and read its report when it finishes; do not poll it. The check prints the mode, the expected duration and the report path (`--report`) when it starts, then progress lines on stderr. The report is headed `mode: full` or `mode: quick`. `--quick` is the fast loop while you iterate (the metric items and a short smoke where every detector judges once; no `--break`): it is never clean. Use it between edits and finish with the full check, which is the gate.

Run the check with `--break` on a target without users: a report without `fails-under-break` does not show that your tests and
detectors notice a broken component, so it is not a clean report. Run it where Service names resolve (in a pod, with
`--cluster-network`); otherwise detectors that dial Service names are not judged and the report says so.

How to read a red item:

- The line names the item, the subject (operation, entry, component) and what was observed. Fix the cause it
  names; never loosen a probe, a threshold or a persistence to turn it green.
- `deployed-healthy` red: read the failed build or rollout first ([serving](../../sdo-serving/SKILL.md)).
- `journeys-move-own-metrics` or `routes-match-operations` red: the metric, its labels or the map entry is wrong
  ([observability](../../sdo-observability/SKILL.md), [map](../../sdo-operations-map/SKILL.md)).
- `fails-under-break` red for an operation: no test or request metric of it failed when its components were
  broken. Make a probe step call the operation and check its result, or make the request metric's route
  label separate it ([detectors](../../sdo-detectors/SKILL.md)). Background work needs a probe that reads its
  effect (a job's output), not only the component's readiness.
- `operations-observed`, `latency-per-operation` red: fix the route value in the map or the metric's labels so the operation's route appears in both the request and the duration series. `background-covered` red: attach the class to the operation whose data it affects, or give it a reason.
- `probes-do-no-harm` red: change the probe, not the threshold.
- SDO's own warm-up raises (a request-metrics baseline still filling on a fresh install) are notes under `quiet-when-healthy`, not red, and never count as noticing a break.
- `quiet-when-healthy` red: a detector fires on a healthy application, your own probes included (a probe that raises while healthy is red). Fix the detector or the probe.
- A check with a matching `reasons` entry in the map is reported as a note. A reason never covers
  `deployed-healthy` or `quiet-when-healthy`.

What green does not prove: a perturbation shows a probe can fail, not that it catches a wrong answer. Each
operation's `correct` note and an independent review of each probe cover content.
