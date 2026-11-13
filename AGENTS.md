# SDO (Self-Defining Operator)

SDO autonomously deploys and operates applications using source-grounded agents, independently validated Go detectors, a long-running Kubernetes controller, and repository-backed operational memory.

## Projects

- [`sdo/`](sdo/AGENTS.md) — agent implementations, typed contracts, operational-memory adapters, and operator code
- `sdo/operator/` — Go detector SDK, execution core, controller runtime and builder, worker registry, commit broker, outcome store, and installer
- [`benchmarks/sregym/`](benchmarks/sregym/AGENTS.md) — first-party SREGym adapters, protocol clients, runners, experiments, and analysis
- [`benchmarks/fuzzops/`](benchmarks/fuzzops/AGENTS.md) — fuzzops adapter: SDO operates a fuzzops app pack while fuzzops injects and grades
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

- `.sdo/` contains the six durable artifact classes: goal, architecture, playbooks, diagnostics, outcomes, and deployment (`.sdo/deployment/`: plan, authored files, telemetry overlay slot, source patches; schema 4).
- Artifact ownership is enforced by the Go commit broker. Agents edit files in isolated checkouts; only validated broker commits reach the one branch of the application repository SDO commits to and deploys from.
- Acceptance of a deployment, health commit or upstream advance is the Go operability report (`sdo/operator/operability`, items O1-O6): a typed, subject-bound aggregate of results SDO's mechanisms already compute, with no verdict field; the broker and every reader recompute `Decide`, and failures route by cause to the deployer, designer, responder or a human. The report is persisted outside `.sdo/` (evidence store, `SDO-Operability` trailer, `sdo-operability` ConfigMap); harnesses read only that JSON.
- The detection designer owns health detectors. Responders add or refine playbooks after a `success` closure; detectors are designer-owned.
- Detector runtime code is deterministic Go using `sdo/operator/sdk`; it must not call an LLM or inspect benchmark verdicts.
- `sdo/operator/runtime` remains transport-neutral. SREGym submission relays and receipts stay behind the benchmark adapter.
- Agent implementations register capabilities through versioned protobuf task/result contracts. The native design assigns operational decisions and accepted repository state to Go; only the storage edge knows the physical outcome layout.
- Native incident/closing-turn tasks carry accepted-commit-bound logical memory snapshots. After Recovered, SDO sends the responder's own session one short closing turn (about 600 bytes, plus the changes Go renders from the recording when `agent.recording` is on; off, the session knows what it did): the agent edits `.sdo/playbooks/` and `.sdo/lib/`, runs the same `validate.Check` the broker uses through `sdo-operator memory check` (and `memory test <id>`), and replies with one line; Go derives the decision, writes the corpus case, derives the match key and credits the playbook. The closing turn also asks the session to keep (commit) or remove leftover app edits, up to `response.cleanup_turns` times (default 2); what remains is recorded as `source_repair_uncaptured` and the outcome is always committed (the loop and dispatch exist; the broker's post-acknowledgement source publication calls them in S4). An escalated, failed or unconfirmed close never learns; a verified (alerts cleared) escalated close still gets the cleanup, the one agent turn an escalation allows. The detection designer also edits final health, traffic and health-objective playbook files directly and returns metadata and logical detector IDs; Go explicitly prepares generic policy detectors in the shared check. A responder changes application source by committing it in its isolated checkout; after a verified closure is acknowledged the broker publishes those commits (and any deployment patches) as one change, Go deploys and verifies it, and only then is it accepted, with the result appended to the outcome as `source_repair`. Uncommitted app files are scratch the broker discards and never deploys, and a deployer's source change is still a patch under `.sdo/deployment/patches/`. Go deploys the effective tree.
- Playbook memory includes responder-owned Go programs under `.sdo/playbooks/`, shared `.sdo/lib/` helpers, and immutable captured `.sdo/corpus/` cases. These remain part of the playbook artifact class. The shared validator enforces provenance, imports, and changed-source formatting before publication; replay outcomes are informational.
- A new install opens no incident, so neither the automatic stage nor the agent runs, until the controller has seen it healthy once or `response.startup_bound` passes; the fact is persisted per install, and a maintenance resume or restart starts no new period.
- SDO applies a learned playbook automatically when its diagnose matches live and every parameter binds (stage [deterministic, agent]); recovery skips the responder; when recovery does not hold within the stage's bound `response.after_playbook` decides (`agent`, the default: the agent finds the stage's writes in `sdo detect` and handles what remains; `escalate`: no agent; `wait`: keep waiting for recovery up to `response.after_playbook_wait`, then escalate), and an alert that did not open the incident never dispatches the agent after an applied playbook; no automatic rollback, no trust gate. A close that did not recover users (failed or escalated) after a playbook applied counts one failure against that playbook revision: trust is an outcome counter that gates nothing, and it is about whether the opening alerts cleared, never about what a repair did to data.
- The agent-cluster recording (the proxy that records every agent Kubernetes call; `memory.AgentWrites` reads it) is off by default (`agent.recording`) and a candidate for removal once a few runs show nothing missing without it. Off, the proxy still forwards, forces the responder field manager and applies the incident fence and the read-only scopes, but keeps no history. Every invariant here holds with it off; the recording only adds writes to what SDO knows (settle, closure guard, repair records, the closing turn's change list). A consumer that needs it must have a fallback.
- An incident opens only on a failing user test (a probe scenario that enters at an exposed entry) or on a Kubernetes failure condition that outlived Kubernetes' own self-healing (its give-up signal, else one self-heal window from a Kubernetes timestamp); every other finding is a clue that never opens or holds an incident. An incident's alert set is its opening alerts plus every user test that starts failing while the incident is open, within `AlertJoinWindow` (5 minutes) of detection (`joined_keys`; clues and Kubernetes conditions never join, and a joiner never re-prompts the agent). Recovered means the whole alert set cleared and stayed clear for the stability window after SDO's last known write settled (its plan applications and the controller's observed settle, plus the agent's recorded writes when `agent.recording` is on; with none known, from the alerts clearing); that one predicate closes the incident, clears the automatic stage and, when it holds, moves the responder's session from incident work to its closing turn. SDO starts the agent once per incident and never re-prompts it for the same symptoms. With the recording off, the learning gate is a `success` closure whose known changes are not all a plain restore (ClosingLearns); the cleanup turn and `source_repair_uncaptured` read the checkout, never the recording. A responder that returns before Recovered held (or whose response timeout passes) escalates: no further agent turn, no learning credit, and the incident stays open until its alerts clear or `sdo-operator incidents reset`.
- SDO refuses no repair for its kind; what a repair did to data is recorded evidence, never a gate or a credit rule.
- A branch head SDO did not write (an engineer's push) is deployed and verified like any new source, held when refused, and never given a trailer. Upstream replay is a benchmark concern: the benchmark adapter pushes replayed commits to that branch like an engineer; the core has no upstream follower, `--source-*` flags or advance merge.
- Program trust is bound to the accepted source revision and independently verified outcomes. Deterministic controller plans, drift assessment, and live reads remain production APIs; benchmark scripted workers, grading, and recording stay under `benchmarks/`.
- The active Go broker stores one versioned outcome document per incident at `.sdo/outcomes/<sha256(incident_id)>.json`. Only the current operational-memory schema is supported; incompatible repositories fail before memory is read. No Python broker remains on the production path.
- Production SDO code, skills and prompts never name specific applications (`scripts/check_app_names.py`; existing uses are a shrink-only ratchet in `scripts/app_name_allowlist.json`).

When changing operational-memory formats or experiment result structures, update `.agents/skills/analyze-experiment/references/`. When moving production or benchmark boundaries, update `docs/architecture.md` (including its component map, artifact ownership matrix, and repository scope table).

Experiment run outputs must stay outside this repository. Run reports and measurements live in the external `sdo-exp` directory, never in repository docs. Run reports and measurements live in the external `sdo-exp` directory, never in repository docs. Do not commit learned programs, generated detectors, cluster captures, corpora, worker results, or model-authored run files. Tests use minimal clearly synthetic fixtures or construct them at test time. Source app inputs are allowed.

## Validation after code edits

```bash
bash scripts/format_code.sh
bash scripts/check_errors.sh
```

`check_errors.sh` includes the AST import and ownership checks in
`tests/unit/test_architecture.py`, including the facade-import rule, alongside
Ruff, Tach, Pyright, Go checks, and proto drift when Buf is installed.

The repository tooling map is [docs/tooling.md](docs/tooling.md).

Use `uv` and `uv run ...` for Python. See [docs/testing-guide.md](docs/testing-guide.md) for focused Python and Go commands.

## Remote repository access

Use `gh` when available for GitHub issues and pull requests.

The repository tooling map is [scripts/README.md](scripts/README.md).
