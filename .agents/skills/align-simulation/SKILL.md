---
name: align-simulation
description: Keep the Go simulation world faithful to real Kubernetes and SDO behavior by finding and closing gaps between them, proactively by running the same scenario in simulation and on a real kind lane and diffing decisions, and reactively when a real or live run shows a bug the simulation cannot reproduce. Use when changing a simulated edge or fault model, after a cluster or live run finds a bug the deterministic tests missed, or on a periodic fidelity check.
---

# Align the simulation with the real world

Deterministic simulation tests are only worth what the simulation models. A gap is real behavior the
simulation cannot represent, or represents wrongly. Every gap found and closed makes the cheap tier catch
a bug that would otherwise cost a kind or live run. Tiers are those of [test-sdo](../test-sdo/SKILL.md).

What the simulation models today, and its accepted limits, live in
[the simulation fidelity doc](../../../docs/simulation-fidelity.md). Read it first; update it with every
gap you close or accept.

## Reactive: a real run found a bug the simulation missed

1. Save the real evidence: controller logs, outcome documents, worker results, and the API, kubelet, log
   or probe observations around the failure. Keep run outputs outside the repository.
2. Reproduce in simulation before changing product code. Choose the seed, fault script and agent script
   that should show it.
3. If it reproduces, the simulation was fine and a test was missing: add the failing seed as a regression
   case, fix the code, rerun the sweep.
4. If it cannot be expressed, that is an alignment gap. Name the real behavior the simulation lacks, from
   the evidence, and classify it ([gap classes](references/alignment.md#gap-classes)).
5. Extend the simulated edge or fault injector to represent it, with a conformance test (below). Then
   reproduce the original bug in simulation, and only then fix the product code.
6. If the behavior is out of scope (see the fidelity doc's bounds), cover the bug with an
   `integration`-tagged test instead and record why the simulation does not model it.

## Proactive: a fidelity check

Run after changing a simulated edge, a fault model or an edge adapter, and periodically.

1. Pick scenarios that exercise the edges you touched, plus the incident classes recent real runs hit.
2. Run each scenario twice with the same fault script and agent script: in simulation, and on a warm real
   kind lane with scripted agents (tier 2a or 2b, no LLM).
3. Record comparable traces from both and diff them, decisions first and edge events second, as
   [the alignment reference](references/alignment.md) describes under Traces.
4. Every difference is a gap candidate. For each, decide: the simulation is wrong (fix it), the real run
   was noisy (explain the noise and make the comparison robust to it), or the difference is an accepted
   bound (record it with its reason).

## Conformance tests

A closed gap gets a conformance test: a small, hand-minimized trace slice of the real behavior, written as
a synthetic fixture (never a copied run output), that the simulated edge must reproduce. These tests are
what stop the simulation drifting back.

## Rules

- Compare decisions (findings, incidents, effects, verdicts) and event order, never wall-clock timestamps or
  generated names.
- Never weaken a product invariant or a test to make simulation and reality agree. Fix the model.
- The simulation may inject more faults than reality shows, never fewer behaviors than it has. A missing
  real behavior is always a gap.
- Model behaviors by mechanism (what the API server, kubelet or network does), not by the one incident that
  revealed them.
- Use one warm kind lane and scripted agents; a fidelity check needs no model calls.
- Keep the gap ledger in the fidelity doc: date, source run, class, status, simulation change and
  conformance test.
