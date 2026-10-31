# SDO experiment evidence reference

Use this reference to correlate durable production evidence with SREGym artifacts. Field sets may grow; inspect the run's schema/version and repository models before assuming optional fields.

## Evidence hierarchy

For production-lifecycle claims, use this order:

1. strict production receipt;
2. controller-owned outcomes and durable runtime state;
3. broker incident ledger and attributable Git commits;
4. lifecycle provenance and validated operational memory;
5. controller, responder, validator, and harness logs;
6. agent-specific trajectories.

A benchmark result can establish the harness verdict. It cannot by itself establish memory ownership, independent verification, reflection, rollout, or cleanup.

## Lifecycle provenance

Current native lifecycle receipts retain goal (when supplied), schema, accepted source,
Go-generated architecture inventory and judge commits. The judge result carries authored,
summary, agent_session_id and logical detector_ids. Drafts carry failure_patterns and detector_ids.
The removed objective_digest field is reserved on the wire. No model architecture session,
round, covered_resources, source-commit echo or topology fingerprint is required.

Historical path: `.sdo/lifecycle-provenance.yaml`

Historical sections (not requirements for current runs):

- `deployer`: session ID, source commit, topology fingerprint, resource inventory, and architecture summary. The model returns only the commit, fingerprint, and summary; the controller attaches the resource inventory deterministically from tracked manifests, so the inventory is a controller fact rather than model output.
- `active_topology` (when supplied by the deployment adapter): sorted kind/name references for the deployed source variant and required non-optional ConfigMap dependencies.
- `health_judge`: the accepted final detector artifact.
- `health_judge_rounds`: ordered structured attempts with distinct session IDs, round numbers, objective digest, covered resources, source, and tests.
- `validation` (when the validator exposes an immutable identity): the diagnostics-tree digest, immutable validator identity, and `sdo.lifecycle-validation/v1` attestation schema. An exact match permits validation reuse without recompilation; a missing or mismatched field requires fresh isolated validation.

For current runs, correlate the accepted source commit with Git and Go's manifest inventory in `arch.md`.
The goal is optional free text; do not require objective digests, coverage echoes or adversarial rounds.
Historical records may retain those fields. A reused lifecycle requires current source and detector validation.

## Detailed evidence by workflow step

- When interpreting mitigation and learning, read [outcome records](outcome-records.md).
- When tracing acceptance and restart behavior, read [native broker and memory](native-broker.md).
- When validating benchmark claims and costs, read [receipts and benchmark results](benchmark-results.md).
- For fastloop rows and learned-program confirmation, read [fastloop results](fastloop-results.md).
- When reconstructing controller timing, read [telemetry and trajectories](telemetry.md).

Detector/corpus snapshot JSON now also carries `statefulSets`, `daemonSets`, `jobs`, `cronJobs`, `persistentVolumeClaims`, `serviceAccounts`, `horizontalPodAutoscalers`, `podDisruptionBudgets`, `ingresses`, `nodes` and `secrets`. Nodes are cluster-scoped observations. Each Secret entry is a value-free projection (`name`, `namespace`, `type`, `labels`, sorted `dataKeys`), not a Kubernetes Secret object; never interpret it as a value capture. Missing lists in older captures mean unrecorded, not proof of absence.
