---
name: sdo-onboarding
description: Onboard an application end to end as SDO's onboarding agent. Covers orchestrating subagents, the goals in a sensible order, sandboxes, the check report and the finish line.
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
Sandboxes are shared and capped: check `sdo sandbox list` before adding one.

## Goals (a sensible order, not steps)

1. It runs: deploy a commit to a sandbox until it is deployed and healthy ([serving](../sdo-serving/SKILL.md)).
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
   Then deploy to the target and keep going until its report is clean.

## Environments

- `sdo sandbox list` shows each sandbox (owner, state, commit) and the target, with each environment's
  Prometheus and tracing URLs. `sdo sandbox add [--owner NAME]` creates one (a kind cluster that mirrors the
  target's settings) and prints its kubeconfig; `sdo sandbox remove N` deletes it. All sandboxes are deleted when
  the session ends.
- A sandbox is disposable: when an experiment leaves it in a state you cannot trust, remove it and add another.
- Change the target only through `sdo deploy <commit> --to target`, never with kubectl or helm directly.

## The loop

1. Commit first. `sdo deploy` refuses a dirty tree, and every report is tagged with the commit it checked.
2. `sdo deploy <commit> --to sandbox-N` builds the commit's images, applies it, waits for rollout and returns the
   check report. `--quick` skips perturbations; run the full check before the target.
3. `sdo journeys run [OP] --on sandbox-N` runs probes once and shows each operation's series before and after.
   `sdo perturb scale-zero|block TARGET --on sandbox-N` breaks one component for a while and shows which probes
   failed and which detectors raised.
4. `sdo onboarding check --on sandbox-N` reruns the check on what is deployed. `sdo map` prints the map;
   `sdo-operator memory check` validates memory the way the broker does.
5. Read the report ([check report](references/check-report.md)), fix the red items, commit and deploy again.

## Finish line

- You are done when `sdo deploy <commit> --to target` reports clean. The target check runs the healthy half
  only (no perturbations); the broker publishes the commit and Go deploys it.
- A red item you cannot fix gets a `reasons` entry in the map (item, subject, one-line reason). A reason never
  excuses `deployed-healthy` or `quiet-when-healthy`, and the report counts reasons, so use them rarely.
- If the session times out, the last target deploy is the result. Deploy to the target as soon as a sandbox
  report is green, then keep improving.
- Reply with one line: `done`, or `blocked: <why>`.

## Skills

[serving](../sdo-serving/SKILL.md), [observability](../sdo-observability/SKILL.md),
[operations map](../sdo-operations-map/SKILL.md), [detectors](../sdo-detectors/SKILL.md).
