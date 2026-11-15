---
name: sdo-onboarding
description: Onboard an application end to end as SDO's onboarding agent. Covers orchestrating subagents, the goals in a sensible order, the check report and the finish line.
---

# Onboarding an application

You make SDO serve this codebase in Kubernetes, healthy, and observable enough that SDO can tell when any
user operation breaks and where. The repository is yours to change: application source, `.sdo/deployment/`,
the map in `.sdo/arch.md`, probes and detectors. Go writes outcomes and lifecycle provenance; leave them alone.
The broker checks only that what you commit is valid (format, schema, compile, tests, patches, secrets).

## You orchestrate

You own finishing. Keep a to-do list in your scratch directory and update it as you learn.
Give each concrete task (deploy, instrument one service, write one operation's probe, find the
entries) to a subagent with a clear finish line. Never accept "done" from the subagent that did
the work: a different subagent reviews it, and the check report confirms it. Read summaries and
reports, not raw logs. When a result is wrong, say why and send it back.

## Goals (a sensible order, not steps)

1. It runs: deploy the commit until it is deployed and healthy ([serving](../sdo-serving/SKILL.md)).
2. You know what users do: every exposed entry, route, non-HTTP entry, background worker,
   schedule and existing record, each written as an operation with what a correct result is
   ([map](../sdo-operations-map/SKILL.md)).
3. Every operation is observable: request, error and latency counts by route; traces across
   services; structured error logs. Reuse first; change code where needed
   ([observability](../sdo-observability/SKILL.md)).
4. The map is true: components and edges from traces and code.
5. Probes cover what traffic can't show: wrong answers, quiet operations, existing records,
   background work. They must pass when healthy and fail when their operation is broken
   ([detectors](../sdo-detectors/SKILL.md)).
6. Fix red items and redeploy. A red item you cannot fix gets a reason in the map.
   Keep going until the report is clean.

## Environment

- There is one environment: the application's cluster, which has no users. Deploy by applying the repository's
  `k8s/render.yaml` (`kubectl apply -f`); commit first, because every report is tagged with the commit it checked.
- Experiments change it. Undo what you break (scale back, delete a policy you added) and check the rollout before
  you trust a result.

## The loop

1. Commit, then deploy and wait for the rollout.
2. Try an operation by hand: send its requests, then read its series in Prometheus and its logs. To see which
   probes and detectors notice a broken component, scale it to zero with `kubectl scale`, watch, and scale back.
3. `sdo-operator onboarding check --help` lists the flags; run it with `--quick` while iterating (about 3 minutes, never clean) and with `--break` (full) as the gate. The full check takes about 10 minutes: start it in the background, keep working, and read its report when it finishes. `sdo-operator memory check`
   validates memory the way the broker does.
4. Read the report ([check report](references/check-report.md)), fix the red items, commit and check again.

## Finish line

- You are done when the full check (not `--quick`, which is never clean) reports clean on the committed tree (with `--break`).
- A red item you cannot fix gets a `reasons` entry in the map (item, subject, one-line reason). A reason never
  excuses `deployed-healthy` or `quiet-when-healthy`, and the report counts reasons, so use them rarely.
- Your reply ends the session, and so does any background check or subagent still running. Reply only when you are
  done or blocked; while a check or a subagent runs, keep waiting (sleep and read its output).
- Reply with one line: `done`, or `blocked: <why>`.

## Skills

[serving](../sdo-serving/SKILL.md), [observability](../sdo-observability/SKILL.md),
[operations map](../sdo-operations-map/SKILL.md), [detectors](../sdo-detectors/SKILL.md).
