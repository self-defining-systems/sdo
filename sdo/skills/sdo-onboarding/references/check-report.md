# The check report

Every `sdo deploy` and `sdo onboarding check` returns one report, tagged with the commit it checked:

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
| `tests-pass-and-fail` | Each operation's probes pass on 3 healthy runs. In a sandbox, each non-store component on its path is scaled to zero once: the probe fails and raises within 60 s, then clears after the undo. The target runs only the healthy half. |
| `quiet-when-healthy` | No detector raises during 10 minutes of healthy journeys. |

How to read a red item:

- The line names the item, the subject (operation, entry, component) and what was observed. Fix the cause it
  names; never loosen a probe, a threshold or a persistence to turn it green.
- `deployed-healthy` red: read the failed build or rollout first ([serving](../../sdo-serving/SKILL.md)).
- `journeys-move-own-metrics` or `routes-match-operations` red: the metric, its labels or the map entry is wrong
  ([observability](../../sdo-observability/SKILL.md), [map](../../sdo-operations-map/SKILL.md)).
- `tests-pass-and-fail` red: the probe does not check its operation's result, or does not reach the broken
  component ([detectors](../../sdo-detectors/SKILL.md)).
- `quiet-when-healthy` red: a detector fires on a healthy application. Fix the detector or the probe.
- A check with a matching `reasons` entry in the map is reported as a note. A reason never covers
  `deployed-healthy` or `quiet-when-healthy`.

What green does not prove: a perturbation shows a probe can fail, not that it catches a wrong answer. Each
operation's `correct` note and an independent review of each probe cover content.
