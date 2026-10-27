# SDO (Self-Defining Operator)

SDO autonomously deploys and operates applications using source-grounded agents, independently validated Go detectors, a long-running Kubernetes controller, and repository-backed operational memory.

## Projects

- [`sdo/`](sdo/AGENTS.md) — agent implementations, typed contracts, operational-memory adapters, and operator code
- `sdo/operator/` — Go detector SDK, execution core, controller runtime and builder, worker registry, commit broker, outcome store, and installer
- [`benchmarks/sregym/`](benchmarks/sregym/AGENTS.md) — first-party SREGym adapters, protocol clients, runners, experiments, and analysis
- `third_party/sregym/` — external SREGym harness Git submodule
- `libs/` — shared agent turn-runner library
- `apps/` — source-deployment evaluation applications
- `tests/` — unit and integration tests

Production code must not import from SREGym packages. Benchmark adapters may import the production API. The production SDO agent core belongs under `sdo/`.

## Development conventions

- Begin with a failing test for bugs and new public behavior; fix bugs as a class ([guide](docs/testing-guide.md#fix-the-class-not-the-instance)).
- Test contracts and ownership boundaries, not private implementation details.
- Use Python type hints, `autopep8`, `ruff`, and `pytest`.
- Prefer dataclasses or Pydantic models over repeated raw mapping shapes.
- Validate configuration in `__post_init__` with `TypeError` or `ValueError`.
- Keep exceptions next to the owning SDO subsystem and preserve actionable context.
- Delete removed code instead of commenting it out.
- Cap fuzz and property-test runs (for example a Hypothesis fuzz profile) at 5 minutes of wall-clock time. Run longer only when the user explicitly asks for a long run.
- Keep code already running in an async loop on that loop: use `async`/`await`, never `run_sync()`, `asyncio.run()`, or `run_until_complete()`.

## SDO invariants

- `.sdo/` contains the six durable artifact classes: goal, architecture, playbooks, diagnostics, outcomes, and rendered deployments.
- Artifact ownership is enforced by the Go commit broker. Agents edit files in isolated checkouts; only validated commits reach the operational branch.
- The health judge owns health detectors. Responders may add or refine incident detectors and playbooks after independently verified outcomes.
- Detector runtime code is deterministic Go using `sdo/operator/sdk`; it must not call an LLM or inspect benchmark verdicts.
- `sdo/operator/runtime` remains transport-neutral. SREGym submission relays and receipts stay behind the benchmark adapter.
- Agent implementations register capabilities through versioned protobuf task/result contracts. The native design assigns operational decisions and accepted repository state to Go; only the storage edge knows the physical outcome layout.
- Native incident/reflection tasks carry accepted-commit-bound logical memory snapshots; responders return logical playbook/script IDs. Reflection directly edits `.sdo/` in its isolated checkout and runs the same `validate.Check` used by the broker through `sdo-operator memory check`. The health judge also edits final health, traffic and health-objective playbook files directly and returns metadata and logical detector IDs; Go explicitly prepares generic policy detectors in the shared check. Source repairs remain isolated checkout edits.
- Playbook memory includes responder-owned Go programs under `.sdo/playbooks/`, shared `.sdo/lib/` helpers, and immutable captured `.sdo/corpus/` cases. These remain part of the playbook artifact class. The shared validator enforces provenance, imports, and changed-source formatting before publication; replay outcomes are informational.
- SDO applies a learned playbook automatically when its diagnose matches live and every parameter binds (stage [deterministic, agent]); the agent is told the result and handles what remains; no automatic rollback, no trust gate.
- Program trust is bound to the accepted source revision and independently verified outcomes. Deterministic controller plans, drift assessment, and live reads remain production APIs; benchmark scripted workers, grading, and recording stay under `benchmarks/`.
- The active Go broker stores one versioned outcome document per incident at `.sdo/outcomes/<sha256(incident_id)>.json`. The memory edge can import the retired JSONL stream for existing repositories; no Python broker remains on the production path.

When changing operational-memory formats or experiment result structures, update `.agents/skills/analyze-experiment/references/`. When moving production or benchmark boundaries, update `docs/architecture.md` (including its component map, artifact ownership matrix, and repository scope table).

Experiment run outputs must stay outside this repository. Do not commit learned programs, generated detectors, cluster captures, corpora, worker results, or model-authored run files. Tests use minimal clearly synthetic fixtures or construct them at test time. Source app inputs are allowed.

## Validation after code edits

```bash
bash scripts/format_code.sh
bash scripts/check_errors.sh
```

`check_errors.sh` includes the AST import and ownership checks in
`tests/unit/test_architecture.py`, including the facade-import rule, alongside
Ruff, Tach, Pyright, Go checks, and proto drift when Buf is installed.

Use `uv` and `uv run ...` for Python. See [docs/testing-guide.md](docs/testing-guide.md) for focused Python and Go commands.

## Remote repository access

Use `gh` when available for GitHub issues and pull requests.
