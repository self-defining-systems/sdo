# Testing guide

SDO spans Python agents, Go operator modules, generated detector workspaces, and optional Kubernetes smoke tests. Validate the narrowest affected contract first, then expand in proportion to risk.

## Test organization

- `tests/unit/sdo/` — agent and contract behavior.
- `tests/unit/benchmarks/sregym/adapter/` — benchmark-adapter transport and receipt behavior.
- `tests/unit/libs/` — shared-library contracts.
- `tests/integration/` — cross-package and external-process behavior.
- `sdo/operator/sdk/*_test.go` — detector SDK and test snapshots.
- `sdo/operator/core/*_test.go` — detector execution and validation.
- `sdo/operator/runtime/*_test.go` — scheduling, persistence, batching, dispatch, broker effects, and state.
- `sdo/operator/sdk/skill_reference_test.go`, `sdo/operator/memory/validate/skills_test.go` — skill drift: generated `sdk.md`, rule citations, and examples (`bash scripts/gen_skill_references.sh` regenerates the reference).
- `sdo/operator/{broker,memory,install,lifecycle}/*_test.go` — accepted state and lifecycle ownership.

## Core commands

```bash
# Format and static checks
bash scripts/format_code.sh
bash scripts/check_errors.sh
./scripts/check_proto.sh

# Python
uv run pytest tests/unit/
uv run pytest tests/integration/
uv run pytest tests/ --cov=sdo

# Go operator modules
(cd sdo/operator && go test ./...)
(cd sdo/operator/sdk && go test ./...)
(cd sdo/operator/core && go test ./...)
(cd sdo/operator/runtime && go test ./...)
(cd sdo/operator/contracts && go test ./...)
```

The proto gate runs `buf lint` and checks that committed Go/Python code matches
regenerated output. The v1alpha1 schema may evolve freely, including breaking
changes; compatibility with main is not a gate. Reserve deleted field numbers
and names so old stored state cannot be misread as new fields.

The operator Go suite includes `TestHermeticResponderProgramReuseThroughRealToolTransport`,
`TestHermeticControllerClosureReachesNativeOutcomeBroker`, and
`TestReflectionLearnedProgramCLIAndBroker`. Together they exercise
the fake Kubernetes API, source repairs, native controller and broker closure,
real responder tool transport, captured corpus phases, direct file editing, shared memory checks
and program compilation, and bounded worker result encoding without a cluster or LLM.
The learned-program test runs the real CLI and broker, checks exact authored-tree acceptance,
and requires identical rejection diagnostics for a README with a physical memory path. Synthetic worker responses exercise contract decoding and the native broker.
New replay fixtures are hand-written toy data or constructed during tests; experiment
outputs stay outside the repository.

Historical plumbing regressions have focused checks in the same suites:

| Regression | Contract check |
| --- | --- |
| Restart live recheck | `TestRestartFastPathRechecksLiveStateAtEveryCrashPoint` covers write, marker and receipt crash points; `TestResponderRestartRediagnosesWithoutUndoOrRestoredHandles` preserves fixes and budgets. `TestStateRoundTripRetainsAppliedActionsAndProgramEvidenceWithoutRollbackState` checks durable outcome evidence without rollback authority. |
| Physical `.sdo/` tool evidence | The responder loop test rejects physical paths in recorded tool calls; `TestSyntheticWorkerResultsThroughContracts` covers typed worker envelopes; responder validation rejects physical tool evidence. |
| Plan apply RBAC | `sdo/operator/install` tests check Service and Deployment get/patch grants. |
| Tool session restart permissions | The responder loop reparses real directory modes; `TestSessionToolServerIgnoresAtomicRequestTemporaryFiles` protects the CLI write protocol. |
| Capture size and timing | `sdo/operator/operatorcli` draft tests reject oversized captures; the responder loop checks detect, settle, and pre-apply snapshots. |
| Pre-existing seed formatting | `TestReflectionLearnedProgramCLIAndBroker` authors final files over accepted memory. |
| ConfigMap and reflection transport size | `TestHermeticFakeConfigMapEnforcesProductionLimit` rejects a state over 1 MiB; `TestReflectionLearnedProgramCLIAndBroker` checks a corpus reference stays below 700 KiB, and `TestRegisteredReflectionWorkerRejectsOversizedResultBeforeCommit` rejects an oversized envelope. |

The no-LLM kind path is documented in
`benchmarks/sregym/experiments/fastloop/README.md`. It is intentionally run by
the experiment lead because it builds images and changes a kind cluster.
The scripted Level 1 check in `tests/unit/benchmarks/sregym/fastloop/`
exercises responder and reflection worker entry points with a copied hotel
seed, typed tasks, and a fake Kubernetes API, without a cluster.
Run `bash scripts/test_fastloop_hermetic.sh` before Level 2. Its Go test drives
two incidents through the real controller, ConfigMap state store, native broker,
logical memory edge, and program builder against a fake Kubernetes API. It
checks a persisted dispatch retry, corpus references and reflection transport
limits, compiled program reuse, verified outcomes, and the deployed image after
source restoration. The gate also runs the real responder tool transport check
and the ConfigMap and reflection size-limit checks. The Python test remains a
fast CLI and result-row check with the scripted worker and fake application API.
The gate needs Go and `uv`, but no cluster or container runtime.

### Tier 2a: toy real cluster

Use the toy commands in [the fastloop README](../benchmarks/sregym/experiments/fastloop/README.md#tier-2a-toy-real-cluster).
This tier needs Docker, kind, kubectl, Go and local runtime images, with no
external SREGym checkout or models. Calico is required: the unchanged production
broker refuses reflection when its default-deny validator canary can reach the API.

Measured on 2026-10-21 with two Go build workers and mounted source:

| Phase | Wall time |
| --- | ---: |
| Prepare node image content (once) | 153 s |
| Create kind cluster | 20.75 s |
| Verify/unpack preloaded images | 59.77 s |
| Install Calico and wait for readiness | 22.77 s |
| Deploy toy app | 3.21 s |
| Install controller and establish adopted clear baseline | 21.79 s |
| Full cold up | 129.73 s |
| Warm reset with Calico and a ready controller | 8.70 s |
| Warm repeat including verification, reflection and evidence export | 14.7 s |
| Periodic sweep of an unowned Service, full incident | 42.3 s |

A schema-v3 integration recheck on the same date measured cold up **126.38 s**,
warm reset **8.95 s**, first incident including invariant acceptance **132.12 s**,
backend/catalog repeats **19.02/16.99 s**, image control **19.52 s**, and an actual
periodic-sweep incident **32.24 s**. All five oracles and strict receipts passed;
the first ledger contains a reflection commit and repeats retain hint bindings.
The incident recheck uses a current scripted-worker dependency image with mounted
source. Cold up used the prepared node's `unified4` images; worker-image build/load
and its 71.53 s controller replacement were measured separately.
Raw runs and logs stay outside Git at
`results/toy-cluster-evidence/`; see the
[validation report](validation/toy-cluster/README.md#integrated-architecture-recheck).

The cold **60-second target remains unmet**. Image transfer/unpacking and initial
controller setup dominate; the toy app itself deploys in about three seconds.
The node preparation script amortizes archive transfer. The first learning
incident took 87.7 s, including isolated validation and controller rebuild; later
live targetPort detection took 0.1 s and mitigation about 3 s. Full wall time
includes exported evidence and learning, and must remain beside those timings.

[Recorded rows, outcomes and broker ledgers](validation/toy-cluster/) show an
initial `drift/investigator` incident, a committed supplied Go invariant, later
`drift/service-targetport-mismatch` findings with `binding_source=hint`, and a
separate event whose source is `periodic program sweep`. The scripted workers
prove integration and invariant reuse, not model learning.

The toy adapter retires a read-only sync Pod only after the real exporter has
finished and requested deletion; this avoids two 30-second sleep termination
waits per repeat. `--no-checkpoints` skips optional offline replay export, whose
investigator capture-reference issue is outside this tier. All real broker,
corpus, validator, outcome and strict-receipt checks remain enabled.

The benchmark unit suite can run without the optional external checkout. Tests
that inspect that checkout's actual worker source or registry explicitly skip
when those fixtures are absent; their assertions run when the checkout exists.

### Fastloop phase budget

Five scripted two-incident dev-loop runs on 2026-10-21 (the `devloop1-*`
results under `fastloop-devloop1`) had these median wall times before the
setup changes. The figures are a budget for finding the next bottleneck, not
an estimate of Kubernetes speed from the fake API.

| Phase | First incident | Second incident |
| --- | ---: | ---: |
| Inventory and lifecycle | 62.7 s | 0.1 s |
| Controller install or reuse inspection | 36.6 s | 32.6 s |
| Controller baseline gate | 31.7 s | 1.1 s |
| Reflection and drain | 271.1 s | 71.3 s |
| Incident wall time | 426.4 s | 143.1 s |

The install inspection is skipped after a fresh lifecycle has already
created a controller Pod. Reused stages must still call the Go installer
inspection: it restores the application namespace's Role/RoleBinding grants
after SREGym recreates that namespace. A saved lifecycle fingerprint and
unchanged Pod UID prove continuity, but do not prove namespace access.
The regression test recreates the namespace at the ClusterOps boundary and
requires grants before resume and fault injection; it also accounts for
that inspection under `controller_install_inspection`. The persistent
baseline gate fails after five minutes, logs progress every 30 seconds,
and includes forbidden informer errors in its readiness failure.

A host-side Go build of `sdk/program` with
`CGO_ENABLED=0`, `GOMAXPROCS=2`, and `GOFLAGS="-mod=mod -p=2"` took 21 s with
an empty cache and under 1 s with the same cache warmed or copied into a
new cache directory. Image build time and in-Pod cache-copy time remain
unmeasured without Docker or kind.

Fastloop result rows now separate `run_prelude_phase_seconds` (first row
only), `driver_phase_timings_seconds`, and `fault_gate_timings_seconds` from
`closure_phase_seconds`. Compare these fields before changing a wait; phase
times from the hermetic fake API and kind are different workloads.

The receipt drain now records acknowledgment and collection timestamps and
durations under `reflection_drain`. The hermetic operator transport test
compares the previous temporary sync Pod path with direct reuse of the
running controller Pod. It checks repository sync and ledger read in both
paths and reports elapsed time and Pod creations; the real cluster's sync Pod
startup cannot be inferred from the fake API timing.

Stage checkpoint tests run entirely offline:

```bash
uv run pytest -q tests/unit/benchmarks/sregym/fastloop/test_replay.py \
  tests/unit/benchmarks/sregym/fastloop/test_model_recording.py \
  tests/unit/benchmarks/sregym/fastloop/test_fake_kube.py \
  tests/unit/benchmarks/sregym/fastloop/test_recording_registration.py
```

For a captured run, `uv run python -m benchmarks.sregym.fastloop replay
--run-dir <results/run-id> --incident 0 --stage broker` reruns the current Go
memory edge and broker in a temporary repository. Reflection replay feeds
recorded model turns through the current agent and recorded direct file edits and the real
corpus-provenance and broker validation, reporting each dispatch's commit or rejection. Responder replay defaults to its scripted worker;
`--agent live` uses recorded Codex turns. Use `--agent scripted` for an explicitly
scripted reflection. A missing turn fails without a model call unless
`--live-on-cache-miss` is selected. Replay does not contact a cluster. See the
fastloop README for checkpoint layout and commands.

Use `scripts/run_tests.sh` for the repository-wide suite.

Pytest and `scripts/test_python.sh` use six workers by default; `pytest-cov` combines the per-worker coverage so the 70% `fail_under` gate still sees the whole suite. Set `SDO_PYTEST_WORKERS=0` for an in-process run through the script, or pass `-n 0` to pytest, when iterating on a single test. Generated detector compilation is covered by the Go builder and broker suites.

Go-to-Python contract shapes are pinned by fixtures under `tests/fixtures/sdo/contracts/go/` (`sdo/operator/runtime/contract_golden_test.go`) and Python's strict models parse them (`tests/unit/sdo/contracts/test_go_contract_fixtures.py`). After an intentional Go encoding change, regenerate them with `SDO_UPDATE_GO_CONTRACT_FIXTURES=1 go test -run TestGoContractFixtures ./...` in `sdo/operator/runtime` and rerun the Python tests. `TestContractShapesNeverEncodeNullCollections` fails on any `null` in those shapes: Python rejects `null` for a list field.

Agent turns are tested with agentshim's `FakeExecutor` through `tests/structured_turns.py`, which replays each provider's real stream format. Tests marked `live_agents` run the real Codex and Claude Code CLIs and are opt-in; cheap models keep them inexpensive:

```bash
SDO_RUN_LIVE_AGENTS=1 SDO_LIVE_CODEX_MODEL=gpt-6-luna SDO_LIVE_CLAUDE_MODEL=haiku \
  uv run pytest -m live_agents tests/
```

Live Codex or Kubernetes tests may require explicit markers, credentials, images, a cluster, and longer timeouts; do not infer production readiness from skipped external tests.

## Test the lifecycle contracts

### Lifecycle separation

Use backend test doubles to prove that source deployment, deployer assessment, and each health-judge round receive only their declared inputs. Verify fresh session identifiers, bounded correction feedback, source commit attribution, and independent acceptance.

### Operational-memory ownership

Create temporary Git repositories with `.sdo/` fixtures. Test:

- human ownership of `goal.md`;
- deployer ownership and topology freshness of `arch.md`;
- responder-only updates under `playbooks/` and incident detectors;
- judge ownership of health detectors;
- append-only controller outcomes;
- path containment, symlink rejection, and commit attribution.

### Detector validation

Generated Go must compile and test in an isolated workspace. Include a matching case and a near-miss case for each incident signature. Validate manifest ownership, class, watches, persistence, batching, provenance, and playbook paths.

### Controller behavior

Prefer deterministic fake clocks, snapshot sources, dispatchers, and state stores. Cover firing and clearing thresholds, debounce windows, restart recovery, idempotent dispatch, exact incident correlation, leader loss, and broker-effect sequencing.

### Benchmark boundary

Tests for `benchmarks/sregym/adapter/` should prove that benchmark resources are added through the runtime extension interface and that strict receipts derive from durable controller/broker evidence. Add architecture tests that reject imports from SREGym packages in production modules.

## Test style

- Write a failing reproducer before fixing a bug.
- Test observable contracts and persisted evidence, not private fields or call order that is not part of the protocol.
- Prefer small explicit fakes over deep mock graphs.
- Use `tmp_path` and temporary Git repositories for repository mutations.
- Mock external subprocess boundaries in unit tests; do not require local `kubectl`, Docker, Go downloads, or model APIs unless the test is explicitly external.
- Assert failure paths: malformed structured responses, timeouts, stale commits, ownership violations, invalid detector code, interrupted rollouts, and duplicate acknowledgements.
- Keep one behavior per test and use names that state the invariant.

## Live validation

Kubernetes smoke scripts validate a different layer from unit tests: image contents, RBAC, storage, network policy, repository synchronization, generated controller startup, responder jobs, and cleanup. Record the exact cluster, images, model, application commit, and command whenever reporting a live result.

Run the deterministic native host, lifecycle, validator, and controller smoke with:

```bash
bash scripts/run_sdo_example_kind.sh
```

It requires Docker, Kind, `kubectl`, `jq`, and Go. Typed fixture workers exercise the Go-owned broker without model credentials. The smoke verifies accepted lifecycle commits, a deployed workload, and clear detector evaluations. Incident response and outcome commits remain a separate live validation gate.

A green unit suite establishes repository contracts. It does not establish that every target application deploys successfully or that an agent repairs every incident.
