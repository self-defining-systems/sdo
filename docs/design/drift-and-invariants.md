# Drift detection and learned invariants

Status: draft for review (2026-10-21). Builds on [playbook-programs.md](playbook-programs.md).

## Problem

SDO opens incidents only when health detectors fire. A latent fault changes configuration without hurting health, so it never fires one. Examples are a Service `targetPort` that no pod serves in an app that routes through Consul, or an edited selector on an unused path. Live runs showed the consequences:

- The no-memory baseline mitigated 2 of 15 target-port faults.
- SDO caught one only when a learned program happened to be swept during an unrelated incident.

Live state also legitimately diverges from source all the time: autoscalers, webhooks, progressive delivery, operational toggles, hotfixes, and SDO's own `recorded-actions` repairs. So "live differs from source" is a question, not a fault.

## Goals

- Detect latent faults deterministically, within seconds of the change, for every fault class SDO has an invariant for.
- Distinguish faults from legitimate drift without a human in the loop.
- Learn only from evidence. New invariants come from verified incidents and investigated drift. Benign drift turns into expected-drift rules and tightened invariants.
- Keep detection and classification in deterministic Go. Agents propose artifacts; the broker validates them.

## Non-goals

- Blocking or reverting drift on suspicion.
- Writing someone else's change into source automatically.
- Learning behavioural properties such as latency or logic bugs from drift labels; health signals and judgement cover those.

## Key constraint

Autonomous labels cannot discover latent faults. A latent fault has no impact by definition, so impact-based labelling would call it benign. Flagging a value as wrong needs either an invariant or a judgement on the specific change. Every invariant comes from a real event:

1. **Incident-derived:** reflection compiles the violated property after a verified incident.
2. **Investigated drift:** a drift investigator explains an unexplained change, calls it a bug with evidence, and the fix is verified. Reflection then compiles the invariant from the captured drift.

Speculative invariant mining (agents proposing checks with no failure behind them, validated only against the healthy state) is rejected as brittle. A single healthy snapshot satisfies many accidental properties, and mutation tests show only that a check can fire, not that it matters.

## Components

### 1. Change feed (controller core)

- The deployer applies with server-side apply under the field manager `sdo-deployer`. The fields SDO's manifests set are the declared intent. Fields other managers own, such as HPA replicas or injected sidecars, are outside it by construction.
- On controller startup or resume, and after an accepted source commit or verified SDO repair, the controller renders accepted manifests through the deployer's renderer with the deployed image and adopts their declared fields using server-side apply as `sdo-deployer` with force-conflicts. It records the adopted object count, taken-over field count, and prior managers once in durable controller status. A dry run is compared with live state before each write. Existing differences are recorded as unexplained drift and left in place for the normal classifier and investigator path; adoption never silently repairs them. Repeating adoption over already-owned fields emits no drift.
- An informer watches the application namespaces. On each add, modify or delete it computes:
  - a field-level diff against the previous object;
  - a diff against the accepted source manifest, normalised through a server-side apply dry run (`DiffAgainstSource`);
  - attribution, by diffing `metadata.managedFields`: manager, operation and time.
- It emits a `DriftEvent{ref, fields, old, new, source, manager, observed_at}`.
- The design does not depend on admission webhooks or audit logs. Either can add user identity later where the cluster allows it.

### 2. Classifier (deterministic, per event)

| Step | Condition | Result |
|---|---|---|
| 1 | Changed fields are not owned by `sdo-deployer` | benign, not drift |
| 2 | An invariant program (Diagnoser) returns `Match` on an affected binding after the change | **fault**: open an incident with `latent` severity; the normal loop takes over (trusted program or responder) |
| 3 | A trusted expected-drift rule classifies it `Benign` | benign, logged |
| 4 | Otherwise | **unexplained**: dispatch the drift investigator (budgeted); the labeller confirms its verdict later |

The program sweep runs **on change** for the affected bindings, with a periodic full sweep as a fallback.

### 3. Drift investigator (agent, budgeted)

Unexplained events are deduplicated by object and field, batched over a short window, and sent to a responder-class worker under a drift task contract. The task carries the object, the changed fields, the old, source and new values, the manager and the captured snapshots. The investigator must return one of:

- **bug**, with concrete evidence: a contradiction with source plus a violated relation (for example, no selected pod serves the port) or observed impact. The normal loop mitigates and verifies, and reflection compiles an invariant program from the drift capture.
- **benign**, with a reason. This is a weak label until the labeller confirms it, and only confirmed cases count toward an expected-drift rule.
- **unsure**: no action, and the labeller resolves it later.

It never reverts on suspicion. "bug" without the required evidence is treated as "unsure". The default hourly token budget is 1,000,000 and applies to re-investigations. Never-investigated drift keys run promptly and before re-investigations even after earlier work exceeds the budget. Budget deferrals carry a typed reason and retry time in controller status and logs. Completed verdicts live in a controller-owned ConfigMap separate from transient controller state, keyed by drift key with observed manager and current/source field values. An unchanged `unsure` or `benign` verdict survives controller restart and fastloop reset; a changed observation can be investigated again. Each verdict should produce a deterministic artifact (an invariant or a drift rule), so the next identical event needs no LLM.

### 4. Autonomous labeller (deterministic)

It resolves unexplained events from observable evidence only:

| Signal | Label |
|---|---|
| Health or SLO detectors fire on the object or its dependents within window W | bad |
| Persists for W with all checks quiet | weakly benign |
| A later accepted source commit adopts the value | strongly benign |
| A later deploy reverts it, and health then degrades | needed |

Each label, with captured before and after snapshots, is stored as a drift corpus case at `.sdo/corpus/drift-<id>/`. Its provenance comes from controller captures, as for incident cases.

### 5. Learning (reflection and maintenance pass)

- **Bad drift (investigator "bug" with a verified fix, or labelled bad) with no covering program:** compile an invariant program through `program scaffold` and `program propose`, with the drift case as its open example.
- **Benign drift that fired an invariant:** add a NoMatch case and tighten the invariant. Repeats lower its trust.
- **Repeated benign drift of one shape:** compile an expected-drift rule (see the SDK section). The trust threshold is K = 3 strongly or weakly benign cases with no bad case.

### 6. Write-back (later phase)

- **SDO's own repairs:** after verification, propose the equivalent source change, validated by the broker. That way SDO-created drift converges.
- **Frequent drift on one field:** propose a structural source change, such as dropping `replicas` when an HPA manages it.

## SDK and contracts

- `DriftEvent` and `drift` corpus cases are new.
- `DriftRule` is a new interface, with the same validation as programs (corpus replay and rename invariance):

  ```go
  type DriftRule interface {
      Spec() ProgramSpec
      Classify(ctx context.Context, v ReadView, e DriftEvent) (DriftVerdict, error) // Benign | Unknown
  }
  ```
- `ReadView.DiffAgainstSource` gets a real implementation backed by the server-side apply dry run.
- New memory artifact class: `.sdo/drift-rules/` (responder-owned, broker-validated).
- Outcome and `sdo status` include drift events and their classification.

## Ownership

| Part | Owner |
|---|---|
| Change feed, classifier, labeller | Controller core (deterministic Go, no LLM) |
| Drift investigator verdicts | Responder-class worker under a drift task contract |
| Invariant programs, drift rules | Proposed by the responder or reflection; validated by the broker |
| Health detectors | Health judge (unchanged) |
| Source write-back commits | Broker, under the `commit` policy |

## Phases

| Phase | Delivers | Unlocks |
|---|---|---|
| P1 | Server-side apply ownership in the deployer; change feed and attribution; sweep on change; drift events in status and outcomes | Latent faults of known classes detected in seconds |
| P2 | Labeller; drift corpus cases | Labelled data; fewer unexplained events |
| P1.5 | Drift investigator: task contract, budget, deduplication, evidence requirement; invariant compiled from a "bug" verdict | First-occurrence latent faults |
| P3 | Learning from drift labels; `DriftRule` | Falling false positives and investigator cost |
| P4 | Write-back of SDO repairs; structural source proposals | Convergence; less recurring drift |

## Evaluation

- **Streams:** add legitimate-drift events (HPA scale, a log-level env toggle, a canary image change) and app-change commits between injected faults. No human acts in any stream.
- **Metrics:**
  - detection latency for latent faults;
  - false-positive incidents on legitimate drift over time;
  - learned invariants and drift rules, with precision and recall against injected faults;
  - LLM tokens per event.
- **Arms:**
  - SDO with the drift investigator;
  - SDO without the investigator, to show what incident-only learning gives;
  - the baseline, given the same source checkout and audit framing;
  - the baseline, also given the same change notifications, to isolate what learning adds beyond the notification.
- **Scoring:** report latent and user-impacting faults separately.

## Risks

- **Investigator false positives.** Mitigated by the narrow question, the evidence requirement, the "unsure" default, and confirmation by the labeller before any rule earns trust.
- **Investigator cost under high drift volume.** Mitigated by ownership filtering, learned rules, deduplication, batching and the budget.
- **Field-manager quality.** Generic or spoofed manager names weaken attribution. The labeller's impact and source-adoption signals do not depend on them.
- **The labeller's window W** trades detection speed against label quality. Start at 10 minutes and measure.
- **Benchmark artifacts.** SREGym injects by mutating live state, which suits drift detection unusually well. Add source-borne fault variants, where the bad change is committed to the app repository, so that results do not rest on drift alone.
