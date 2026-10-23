---
name: analyze-experiment
description: Analyze SDO and SREGym experiment runs using lifecycle provenance, operational-memory outcomes, broker/controller evidence, strict production receipts, benchmark result CSVs, and agent-specific trajectories when present. Use when asked to review or compare experiment runs, explain deployment or incident behavior, find detours, assess controller/responder evidence, or invoke /analyze-experiment.
---

# Analyze experiment

Analyze a run from durable evidence outward. Do not assume that a process exit code, agent log, or benchmark score proves the production lifecycle completed.

## Locate evidence

SREGym runs normally live under `third_party/sregym/logs/`:

- single run: `<timestamp>_<agent>/`
- pipeline: `<timestamp>_pipeline_<name>/stage_<index>_<name>/`
- per problem: `problem_runs/<problem-id>/`

Resolved `experiment_config.toml`, `tasklist.yml`, `pipeline_config.toml`, and `pipeline_state.json` establish configuration and stage state. Per-problem result CSVs, `agent/` artifacts, benchmark logs, `application_workspace/`, and pipeline lifecycle seeds provide run evidence.

For SDO adapter runs, prioritize:

1. `agent/sdo_production_receipt_strict.json`;
2. the application workspace's `.sdo/outcomes/` documents and `.sdo/lifecycle-provenance.yaml`;
3. broker incident ledgers in the Git common directory when preserved;
4. controller/responder Kubernetes logs or copied logs;
5. benchmark result CSVs and harness logs.

Agent-specific trajectories may exist for generic CLI benchmark agents. Treat them as explanatory evidence, not the authoritative SDO record.

## Workflow

1. Read the resolved configuration and pipeline state.
2. Inventory every problem run and its result CSV before selecting deep dives.
3. For an SDO run, validate the strict receipt shape and correlate its incident and commits with lifecycle provenance, outcomes, and broker evidence.
4. Reconstruct the timeline: lifecycle bootstrap or reuse, detector firing, batching, dispatch, proposal, independent verification, outcome, reflection, rollout, acknowledgement, and cleanup.
5. Compare agent logs or trajectories with authoritative state. Flag contradictions instead of choosing the more favorable source.
6. Identify failed invariants, detours, repeated hypotheses, stale memory, uncorrelated commits, or missing evidence.
7. Report configuration, evidence paths, findings, and confidence. Separate production-lifecycle completion from benchmark diagnosis/mitigation scores.

When multiple runs are in scope and subagents are available, assign one run or one evidence layer per subagent, then reconcile incident IDs, commits, timestamps, and classifications centrally.

## Report structure

1. **Result** — what is proven, not merely claimed.
2. **Configuration** — resolved agent, model, problem set, stage, workspace mode, and relevant flags.
3. **Evidence chain** — receipt, provenance, controller/broker state, outcomes, and benchmark results.
4. **Timeline** — lifecycle and incident events in order.
5. **Detours or failures** — root causes and wasted work with source paths.
6. **Comparison** — normalized metrics and behavior differences when multiple runs are requested.
7. **Recommendations** — concrete changes or follow-up experiments.

At evidence discovery, read [evidence hierarchy and provenance](references/trajectory-schema.md).
Before judging mitigation or learning, read [outcomes and program evidence](references/outcome-records.md).
For menu selections, round reports and recovery receipts, read [mitigation menu receipts](references/mitigation-menu.md).
For first-mutation capture and evidence-gap semantics, read [capture boundaries](references/capture-boundaries.md).
For acceptance, ownership and restart analysis, read [native broker and memory](references/native-broker.md).
Before validating benchmark verdicts or costs, read [receipts and benchmark results](references/benchmark-results.md).
For fastloop timing, trust and learned findings, read [fastloop results](references/fastloop-results.md).
Before judging mixed-app slot validity or recovery, read [slot cleanup evidence](references/slot-cleanup.md).
Before reconstructing a timeline, read [telemetry and trajectories](references/telemetry.md).
When classifying behavior, read [common failures](references/failure-patterns.md); for latent drift or toy lanes,
also read [drift and toy failures](references/drift-failures.md).
