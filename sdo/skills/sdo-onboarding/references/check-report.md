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
| `fails-under-break` | Run with `--break`, on a target without users. Each component on an operation's path is scaled to zero once (components that share no operation are broken together in one wave, so the item takes a few waves rather than one break per component; a raise must still name the operation or component) (or denied ingress by a NetworkPolicy with `--break-mode network` when the CNI enforces it; otherwise it falls back to scale-to-zero and says so) and restored. An operation is green when a raise (within `--break-bound`, 60 s) names the operation or one of its probe scenarios. A generic detector that only names the broken component (for example `service-endpoints`) leaves the operation red: add or fix the test that exercises it. An unverifiable restore is red and stops the item. It runs even when `quiet-when-healthy` is red; raises standing before a break are ignored. Each line says whether a failing-request signal or only an absence signal (an operation or its series gone) named the operation; prefer a test that fails a request. |
| `operations-observed` | Every declared operation's route appears in its entry metric (no `reasons` entry excuses a missing one; the line names the nearest observed label), and most of an entry's real traffic goes to declared routes. |
| `latency-per-operation` | A request-duration series carries each operation's route. |
| `background-covered` | Every job or queue class in the app's job metrics belongs to a declared operation (set its `route` to the class name). |
| `probes-do-no-harm` | With the memory's probes running, no operation or job class fails more than with them paused, and the probes are not a large share of an entry's requests. The line names the failing operation or class and the probe scenarios; fix the probe so it leaves no objects the application's work then trips on. `--harm-window` (1 minute, 0 skips). |

With `--break`, `fails-under-break` also runs two breaks that keep every component up, each judged the same way (a raise within the bound must name an operation it hurts; every one is restored): `worker-quiet` (a worker stays Running and Ready but cannot reach anything except DNS; needs a CNI that enforces NetworkPolicy, otherwise a note) and `data-wrong` (a text column of the oldest rows of the most-read table of a postgres store is overwritten). A pod being up does not show them: a probe must read the result.

Each live item reports its duration (`time <item> <duration>`). The check prints the mode, the expected duration and the report path (`--report`) when it starts, then progress lines on stderr. The report is headed `mode: full` or `mode: quick`. `--quick` is the fast loop while you iterate (the metric items and a short smoke where every detector judges once; no `--break`): it is never clean. Use it between edits and finish with the full check, which is the gate.

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
- `operations-observed`, `latency-per-operation` or `background-covered` red: fix the route value in the map or the metric's labels so the operation's route appears in both the request and the duration series.
- `probes-do-no-harm` red: change the probe, not the threshold.
- SDO's own warm-up raises (a request-metrics baseline still filling on a fresh install) are notes under `quiet-when-healthy`, not red, and never count as noticing a break.
- `quiet-when-healthy` red: a detector fires on a healthy application. Fix the detector or the probe.
- A check with a matching `reasons` entry in the map is reported as a note. A reason never covers
  `deployed-healthy` or `quiet-when-healthy`.

What green does not prove: a perturbation shows a probe can fail, not that it catches a wrong answer. Each
operation's `correct` note and an independent review of each probe cover content.
