---
name: sdo-operationalize
description: Use when deciding what an application's SDO detectors and synthetic traffic should watch - deriving a complete, cheap health detector set from the application's source at bootstrap, or adding detectors after an incident showed a gap. Maps each check to the SDK input that can express it and lists what is already installed.
---

# Operationalizing an application

This skill decides what to monitor. Write each detector with the `sdo-detector-authoring` skill and use the
`sdo-memory` commands. Detectors page agents, not people: a false alarm costs one agent turn, while a missed or
unlocalized fault costs the whole incident. So aim for high recall and precise evidence at low runtime cost.

## Two rules generate the detector set

1. **Declared versus actual.** Everything the source declares must be observed as declared: workloads, ports,
   selectors, config references and keys, service-to-service calls, routes and seeded data.
2. **Healthy versus now.** Everything measured on the healthy application stays within its healthy range.

## Procedure

1. Inventory the source and note the file each fact came from:
   - user-facing routes (HTTP handlers, routers) and what a correct response contains;
   - internal APIs and their protocols;
   - outbound calls (client dials, env, args or config values naming a Service and port);
   - data stores and queues the code uses;
   - config files and the keys the code reads, with their formats;
   - records the application seeds at startup;
   - error strings the code logs, and log lines the healthy application always emits;
   - metrics the code exports, and whether the source deploys a Prometheus;
   - caller timeouts and deadlines.
2. List what already runs, so you do not duplicate it: `sdo-operator memory ls --kind detector`, and the
   Go-installed checks under "Already covered" in the [detector catalog](references/catalog.md).
3. Walk the [detector catalog](references/catalog.md) layer by layer. Write every item that applies to
   something in your inventory, using the input the catalog names. Every layer your inventory touches gets a
   detector or a line under `Not monitored` saying why it cannot be observed; lack of time is not a reason.
4. User journeys come first. Every user-facing route gets a scenario that checks the response body. Every write
   route gets a journey that writes synthetic data, reads it back and cleans up. A read-only scenario set for an
   application with write paths is incomplete.
5. Set firing, severity and thresholds as [methods](references/methods.md) describes.
6. Test each detector: a case that fires, near misses that stay quiet, and a healthy case, with resource names
   different from any you saw.
7. Record blind spots. For each inventory item that no available input can observe (catalog "Not expressible
   yet"), add a line under a `Not monitored` heading in the health-objective playbook. Name the item and the
   missing input, so responders know where detectors cannot see. Never ship a check that cannot observe its
   condition.
8. Run `sdo-operator memory check` until it exits 0.

## Principles

- Symptoms decide health; causes localize. Write both. A journey check says users are affected. A cause check,
  such as "a Service selector matches no pods", says where. The responder needs both.
- One detector per mechanism, and one finding per affected object with a stable fingerprint. Then the controller
  folds the findings of one fault into one incident instead of starting several agents on it.
- The `health-objective` detector has a fixed spec. Put checks that need other watches (Events, usage, logs, the
  newer workload kinds, metrics) or another severity in separate health detectors with their own IDs, each listed
  in your result. A typical application needs several: warning events, resource usage, and logs when the source
  logs errors.
- Evidence is the agent's only view of the alert. Put the observed value, the expected value, the source of the
  expectation and how it was computed in `Finding.Evidence`.
- Cheapest input first: snapshot reads are free; traffic costs requests; usage, logs and metrics cost one read
  per cycle. Never probe
  faster than the judge task's traffic limits.
- Derive every expectation from observed relations or from the source, never from one deployment's names or
  values. A check must stay true after a legitimate redeploy.
- Never weaken a user-journey check to silence it. Fix the threshold or the scenario instead.

## After an incident (reflection)

- A fault that only a journey check caught, with no cause check naming it: add an incident detector for its
  mechanism from catalog layers 2-7. The next occurrence is then caught earlier and localized.
- A detector that fired while nothing was wrong: tighten it with the outcome's evidence. It must still fire on
  its captured cases and stay quiet on recorded healthy baselines.
- Responders own incident detectors and the health judge owns health detectors. The `sdo-detector-authoring`
  rules enforce this.
