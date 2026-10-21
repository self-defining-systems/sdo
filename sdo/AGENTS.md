# SDO production packages

`sdo/` contains Python agent implementations and versioned contracts alongside the Go operator. The active `sdo-operator host` and `sdo-operator operate` commands run the Go lifecycle, controller, broker, installer, and operational-memory edge. Python implements the registered workers and their model sessions.

## Package map

| Package | Responsibility |
|---|---|
| `agents/deployer/`, `agents/health_judge/`, `agents/responder/` | Built-in agent implementations. They receive typed tasks, work in proposal checkouts, and return typed results. Their Codex/Claude choices are local implementation details. |
| `agents/registrations/` | Example capability and launch manifests for built-in agents; the Go registry accepts other implementations that obey the same contracts. |
| `agents/lifecycle/` | Shared source architecture and health judge agent prompts and DTOs; operational decisions remain in Go. |
| `contracts/`, `contracts/proto/` | Generated Python/Go protobuf contracts and their shared schema. Agent task/result envelopes use ProtoJSON and typed `Any` payloads whose URLs are checked against registration. |
| `operator/worker_registry/` | Go registration validation, capability selection, and generic Kubernetes worker execution. Unknown agents or task/result types fail closed. |
| `operator/lifecycle/`, `operator/deployment/`, `operator/artifacts/` | Go lifecycle coordinator, trusted deployment strategies, and the physical bootstrap-artifact writer. |
| `operator/broker/`, `operator/memory/`, `operator/outcome_derivation/` | Go commit authority, logical outcome interfaces and physical JSON repository adapter, and controller-derived outcome records. |
| `operator/{sdk,core,runtime,builder,install}/` | Deterministic detector API and execution, long-running controller, build/supervision, and Kubernetes installation. |

## Durable memory and ownership

The five durable artifact classes remain `.sdo/goal.md`, `.sdo/arch.md`, `.sdo/playbooks/`, `.sdo/diagnostics/`, and outcomes. Agents edit isolated checkouts; reflection authors final memory files and returns logical IDs; the judge retains typed bootstrap artifacts; only the Go broker validates and publishes commits. The health judge owns health diagnostics, responders own incident diagnostics and playbooks, and the controller owns outcomes.

The Go `operator/memory` interface reads outcomes by incident ID and does not expose filenames. Its JSON adapter stores one versioned document per incident at `.sdo/outcomes/<sha256(incident_id)>.json` and can import the retired `.sdo/outcomes.jsonl` stream. Agent requests carry logical memory snapshots. Reflection edits final files, checks them with `sdo-operator memory check`, and returns decisions and logical playbook IDs. Only the judge uses the existing typed bootstrap writer. The broker derives the diff, independently repeats the shared check and alone publishes accepted commits.

## Development rules

- Write a failing contract test before changing public behavior. Preserve fresh deployer, judge, responder, and reflection sessions where their roles require separation.
- Keep agent tasks language neutral. An agent may edit only its proposal checkout, never the accepted repository or broker-owned `.sdo/` outcomes.
- Run generated detector code only through isolated validation and deterministic Go controller execution. Keep benchmark verdicts, submission relays, and receipts under `../benchmarks/sregym/`.
- Keep existing async Python handlers on their event loop.
- Update `../docs/architecture.md` and `../.agents/skills/analyze-experiment/references/` when ownership, layout, or evidence interpretation changes.

Validate Python edits with root formatting/lint scripts and focused tests under `tests/unit/sdo/`; validate Go packages from their respective `sdo/operator/` modules.
