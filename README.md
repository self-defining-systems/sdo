# SDO: Self-Defining Operator

[![arXiv](https://img.shields.io/badge/arXiv-2610.14547-b31b1b.svg)](https://arxiv.org/abs/2610.14547)

SDO deploys an application from its source code and then keeps it running. You give it a repository and a plain-language health objective ("the frontend serves requests; every Deployment is available"). SDO deploys the application to Kubernetes, writes Go detectors that check that objective, and runs a long-lived controller that watches the application and sends a coding agent to repair it when a detector fires.

SDO doesn't trust its agents to grade their own work:

- **Detectors are deterministic Go, checked independently.** Agents write detectors, but SDO reruns validation itself before one ships. It compiles the detector in a no-network sandbox and rejects detectors that would fire on the application's healthy structure. An agent's own self-check never counts as evidence. At runtime, detectors never call a model.
- **Agents propose; a broker commits.** Every agent works in an isolated Git worktree. A commit broker enforces who may change which artifact, and only validated changes reach the application's operational branch.
- **Memory grows from verified outcomes.** After an incident, SDO checks recovery itself, records the outcome, and lets the responder add incident detectors and playbooks. The next incident of the same kind starts from that memory.

SDO is described in the paper [A Self-Defining Operator for Distributed Applications](https://arxiv.org/abs/2610.14547).

> **Status:** research prototype. Interfaces and on-disk formats may change without notice.

## How it works

```text
sdo-operator host
  -> source deployer          deploys the application from its repository
  -> health judge             turns the objective into Go health detectors
  -> .sdo operational memory  goal, architecture, detectors, playbooks, outcomes
  -> detector validation      compiles and checks detectors in a sandbox
  -> Kubernetes controller    evaluates detectors on cluster changes
  -> incident responder       a coding-agent session that diagnoses and repairs
  -> broker and reflection    validated commits, recorded outcomes, learned detectors
```

The deployer, health judge, and responder are separate agent sessions (Codex or Claude). The controller is a Go program that runs in the cluster. It dispatches each responder as a Kubernetes Job, and it never calls a model itself.

## Prerequisites

- Python 3.10 or newer, and [uv](https://docs.astral.sh/uv/)
- Go 1.26 or newer (for the controller and detectors)
- Docker, [kind](https://kind.sigs.k8s.io/), and `kubectl`
- The [Codex CLI](https://github.com/openai/codex) or [Claude Code](https://docs.anthropic.com/en/docs/claude-code), with credentials for it

## Quick start

Clone the repository and install dependencies. You don't need the submodules to run SDO itself, only for the evaluation applications and the SREGym benchmark.

```bash
git clone <repository-url> sdo
cd sdo
uv sync
```

### Run one incident end to end in kind

```bash
bash scripts/run_sdo_example_kind.sh
```

This script:

1. Builds the SDO images (`scripts/build_sdo_images.sh`).
2. Creates a kind cluster with Calico for network policies.
3. Runs the native Go host and lifecycle with deterministic typed fixture workers.
4. Independently validates and deploys the proposed source and health detector.
5. Starts the Go controller and verifies clear detector evaluations.

The smoke uses no model credentials. It covers native bootstrap and controller readiness; incident response and outcome commits require a separate live check.

### Operate your own application

Build the images into your local Docker image store, or push them to a registry your cluster can pull from:

```bash
bash scripts/build_sdo_images.sh
```

Point the Go host at a clean Git repository, a health objective, and the cluster resources it will use:

```bash
sdo-operator host --repository /path/to/application \
  --namespace my-app \
  --application my-app \
  --goal-file /path/to/health-objective.md \
  --operation-id my-app-bootstrap \
  --controller-image sdo-controller:v0.1.0 \
  --responder-image sdo-responder:v0.1.0 \
  --validator-image sdo-detector-validator:v0.1.0 \
  --buildkit-image 'moby/buildkit@sha256:<pinned-digest>' \
  --repository-pvc sdo-application-repository \
  --credentials-secret sdo-agent-credentials \
  --registry-push-secret sdo-registry-push \
  --model gpt-6-luna
```

The full interface:

```text
sdo-operator host --repository REPOSITORY --namespace NAMESPACE
  --application NAME (--goal TEXT | --goal-file PATH)
  --operation-id ID --controller-image IMAGE --responder-image IMAGE
  --validator-image IMAGE --buildkit-image DIGEST_PINNED_IMAGE
  --repository-pvc PVC --credentials-secret SECRET
  --registry-push-secret SECRET --model MODEL
```

Use `sdo-operator host --help` for optional timeouts, agent registration,
registry targeting, reflection, and bounded-controller settings. The credential
and registry push Secrets must already exist in the control namespace.

The health objective always comes from you. SDO never infers it from the application or from benchmark results.

## Operational memory

Each operated application carries its operational memory in `.sdo/`, versioned alongside the application's source:

```text
.sdo/
├── goal.md          # health objective (human-owned)
├── arch.md          # source and topology summary (deployer-owned)
├── playbooks/       # repair procedures (responder-owned)
├── diagnostics/     # Go health and incident detectors, plus a manifest
└── outcomes/        # one incident outcome record per file (controller-owned)
```

The commit broker enforces ownership. For example, a responder can add playbooks and incident detectors, but it cannot rewrite the goal, the health detectors, the architecture summary, or past outcomes.

## Reproducing the SREGym evaluation

`benchmarks/sregym/` connects SDO to the [SREGym](https://github.com/vic-lsh/SREGym) fault-injection benchmark, which is vendored as the `third_party/sregym` submodule. Two example configurations show the comparison:

- `sdo_example.toml` runs SDO on two identical incidents. The second incident sees the memory from the first.
- `codex_baseline_example.toml` runs a memoryless Codex agent on the same problems.

```bash
git submodule update --init third_party/sregym

# Check that the two arms are comparable (model, effort, judge, cluster settings)
uv run python -m benchmarks.sregym.runner.preflight \
  benchmarks/sregym/experiments/sdo_example.toml \
  benchmarks/sregym/experiments/codex_baseline_example.toml

uv run python -m benchmarks.sregym.run benchmarks/sregym/experiments/sdo_example.toml
uv run python -m benchmarks.sregym.run benchmarks/sregym/experiments/codex_baseline_example.toml
```

To resume a run, pass its log directory instead: `uv run python -m benchmarks.sregym.run third_party/sregym/logs/<run-dir>/`. The benchmark code is an adapter around SDO: production packages under `sdo/` and `controller/` never import from it.

## Repository layout

| Path | Contents |
|---|---|
| `sdo/` | Python runtime: lifecycle (deploy, architecture, health judge), responder, contracts, operational memory and commit broker, controller installation, `sdo` CLI |
| `sdo/operator/sdk/` | Public Go API for writing detectors |
| `sdo/operator/core/` | Detector execution and snapshot validation |
| `sdo/operator/runtime/` | The long-running controller: scheduling, findings, batching, responder dispatch, durable state |
| `sdo/operator/builder/` | Builds and validates an application-specific controller from `.sdo/diagnostics` |
| `proto/` | Shared contract schemas; Go and Python code is generated from them |
| `libs/` | Shared helpers: the structured agent-CLI runner, model configuration, neutral runtime utilities |
| `benchmarks/sregym/` | SREGym adapter, protocol clients, experiment runner, fast-loop harness, analysis |
| `apps/` | Applications used for source-deployment evaluation (Git submodules) |
| `tests/` | Unit and integration tests |

[docs/architecture.md](docs/architecture.md) covers component boundaries, artifact ownership and the controller lifecycle. [docs/feature-flags.md](docs/feature-flags.md) lists the opt-in runtime switches.

## Development

```bash
bash scripts/format_code.sh      # ruff format
bash scripts/check_errors.sh     # ruff lint, tach architecture boundaries, proto checks
uv run pytest tests/unit/
(cd sdo/operator/runtime && go test ./...)
```

[docs/testing-guide.md](docs/testing-guide.md) has the full set of Python and Go test commands, including integration tests and the contract fixtures shared between Go and Python.

## Citation

If you use SDO in your research, please cite:

```bibtex
@misc{li2026selfdefiningoperatordistributedapplications,
      title={A Self-Defining Operator for Distributed Applications},
      author={Shihang Li and Thomas Anderson and Ratul Mahajan and Simon Peter and Luke Zettlemoyer},
      year={2026},
      eprint={2610.14547},
      archivePrefix={arXiv},
      primaryClass={cs.DC},
      url={https://arxiv.org/abs/2610.14547},
}
```

## License

SDO is released under the [MIT License](LICENSE).
