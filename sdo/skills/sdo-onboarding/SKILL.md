---
name: sdo-onboarding
description: Onboard an application end to end as SDO's onboarding agent. Covers orchestrating subagents, the goals in a sensible order, the check report and the finish line.
---

# Onboarding an application

You make SDO serve this codebase in Kubernetes and detect broken user operations. Preserve the supplied
`.sdo/goal.md`. On a provided deployment, commit only files the initial health judge owns. Edit `.sdo/arch.md`
inside its application-map markers. Go owns the rest of the architecture, schema, outcomes, and lifecycle provenance.

## You orchestrate

You own finishing. Keep a to-do list in your scratch directory and update it as you learn.
Give each concrete task (deploy, instrument one service, write one operation's probe, find the
entries) to a subagent with a clear finish line. Never accept "done" from the subagent that did
the work: a different subagent reviews it, and the check report confirms it. Read summaries and
reports, not raw logs. When a result is wrong, say why and send it back.

## Goals (a sensible order, not steps)

1. It runs: deploy the commit until it is deployed and healthy ([serving](../sdo-serving/SKILL.md)).
2. You know what users do: every exposed entry, route, non-HTTP entry and existing record, each written as an
   operation (background work belongs to the operation that triggers it) with what a correct result is
   ([map](../sdo-operations-map/SKILL.md)).
   A separate subagent whose only job is finding missing journeys (from source, tests, docs and API specs, given
   the current map) reviews the map; fix what it finds or exclude it with a reason.
3. Every operation is observable: request, error and latency counts by route; traces across
   services; structured error logs. Reuse first; change code where needed
   ([observability](../sdo-observability/SKILL.md)).
4. The map is true: components and edges from traces and code.
5. Probes cover what traffic can't show: wrong answers, quiet operations, existing records,
   background work. They must pass when healthy and fail when their operation is broken
   ([detectors](../sdo-detectors/SKILL.md)).
6. Fix red items and redeploy. A red item you cannot fix gets a reason in the map.
   Keep going until the report is clean.

## What done means for each journey

The people running the application give the user journeys SDO must support; they are authoritative. They are in your
goal under "User journeys:" as the people wrote them (free text, often one journey per line), and goal.md's front
matter lists the same journeys as `id`/`what`. Cover every one fully: each has at least one operation with
`journey: <id>` (use the front matter's ids), and each such operation meets the whole checklist below. A journey you
cannot find in the application is a blocker you report, never one you drop. Still map every route: a route outside
the given journeys is its own operation (with no `journey` field) or an excluded route with a reason. When the goal
lists no journeys, find them yourself.

Every user journey is instrumented and detected: mapped in the app's own metrics (with the background work it
triggers), errors and latency per journey, a probe that checks its result end to end, a detector that names it
when any component on its path breaks, a true component path with metrics, logs and trace pointers. The
check prints one row per journey; the items are in [the standard](references/journey-standard.md). Instrument in
application code (middleware, metrics, logging, tracing) or the deployment, whichever gives the items; install and
configure OpenTelemetry yourself where it helps.

## Environment

- There is one environment: the application's cluster, which has no users. Deploy by applying the repository's
  `k8s/render.yaml` (`kubectl apply -f`); commit first, because every report is tagged with the commit it checked.
- Experiments change it. Undo what you break (scale back, delete a policy you added) and check the rollout before
  you trust a result.

## The loop

1. Commit, then deploy and wait for the rollout.
2. Try an operation by hand: send its requests, then read its series in Prometheus and its logs. To see which
   probes and detectors notice a broken component, scale it to zero with `kubectl scale`, watch, and scale back.
3. `sdo-operator onboarding check --help` lists the flags. Without `--prometheus` it runs SDO's own Prometheus for you, so do not deploy one by hand. Run it with `--quick` while iterating (about 3 minutes, never clean) and with `--break` to check breaks. After the first `--break` run, later runs in the session reuse the verdicts of items whose probes, detectors, map entries and deployment did not change (labelled `reused from`; report `mode: incremental`, never clean), so a rerun after a one-probe change takes a few minutes. While fixing red items, loop on `--quick` and incremental `--break`; do not rerun `--full --break` for each fix. `--full --break` ignores the record and is the gate, run once at the end to confirm clean (it takes 10-20 minutes: start it in the background, keep working, and read its report when it finishes). `sdo-operator memory check`
   validates memory the way the broker does.
4. Read the report ([check report](references/check-report.md)), fix the red items, commit and check again.

## Finish line

- You are done when one full check (`--full --break`; `--quick` and incremental runs are never clean) reports clean on the committed tree. Run it once, after the loop is green, not as the loop.
- Commit all export edits and run `sdo-operator memory check`. If the session reports a rejected export, fix the diagnostics and commit again.
- A red item you cannot fix gets a `reasons` entry in the map (item, subject, one-line reason). A reason never
  excuses `deployed-healthy` or `quiet-when-healthy`, and the report counts reasons, so use them rarely.
- Reply with one line: `done`, or `blocked: <why>`.

## Skills

[serving](../sdo-serving/SKILL.md), [observability](../sdo-observability/SKILL.md),
[operations map](../sdo-operations-map/SKILL.md), [detectors](../sdo-detectors/SKILL.md).
