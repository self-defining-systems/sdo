# Testing guide

SDO spans Python agents, Go operator modules, generated detector workspaces, and optional Kubernetes smoke tests. Validate the narrowest affected contract first, then expand in proportion to risk.

## Test organization

Memory-host RPC tests must prepare and activate a host through Manager and capture
its mirror barrier. Inline MemoryObservation snapshots are unsupported. Traffic
failure checks include `TestTrafficActivationFailureAndCrashRecovery` (manager
scheduler-start rejection and disconnect recovery) and
`TestSimMemoryTrafficTwoRevisions` (bufconn engine parity).

- `tests/unit/sdo/` — agent and contract behavior.
- `tests/unit/benchmarks/sregym/adapter/` — benchmark-adapter transport and receipt behavior.
- `tests/unit/libs/` — shared-library contracts.
- `tests/integration/` — cross-package and external-process behavior.
- `sdo/operator/sdk/*_test.go` — detector SDK and test snapshots; `sdk/metrics` covers metric and alert detectors against `sdktest.Snapshot.Metrics`/`Alerts` fixtures.
- `sdo/operator/core/*_test.go` — detector execution and validation.
- `sdo/operator/runtime/*_test.go` — scheduling, persistence, batching, dispatch, broker effects, and state; `metrics_observer_test.go` and `runtime/prometheus/` run the metrics observer and adapter against an `httptest` fake Prometheus.
- `sdo/operator/sdk/skill_reference_test.go`, `sdo/operator/memory/validate/skills_test.go` — skill drift: generated `sdk.md`, rule citations, and examples (`bash scripts/gen_skill_references.sh` regenerates the reference).
- `sdo/operator/{broker,memory,install,lifecycle}/*_test.go` — accepted state and lifecycle ownership.

## Core commands

The temporary memory-host, restart-after-closure, supervise and prober launch flags
are rejected. Builder tests assert that generated controllers import runtime only.
AcceptedDetectorProbe passes the validated host artifact and accepted revision to
bounded health evaluation. Synthetic embedded tests inject their transport;
production always uses the active host manager. Responder verification tests cover
`verify --workload ... --scenario ...` through incident transport and host RPC.

The static Go compilation gate builds production packages without test files before
benchmark vet, catching production declarations accidentally moved into tests.

The integration host build test also checks real-process Ready startup with a
synthetic kubeconfig, preserving bounded health evaluation outside a pod.

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
(cd benchmarks/sregym/protocol/native_receipt_go && go test -race ./...)
(cd benchmarks/sregym/protocol/submission_go && go test -race ./...)
```

The Go commands above run the deterministic unit tier. Real Git, compiler and
socket contracts require `-tags=integration`. Both PR and nightly
`scripts/test_go_ci.sh` select that tag; `scripts/run_tests.sh` and
`scripts/test_fastloop_hermetic.sh` also retain it so their full/smoke selections
continue to execute migrated process tests. The benchmark replay module is
included in the full runner and both CI modes.

`scripts/check_errors.sh` runs Ruff, Tach, the AST import and ownership checks in
`tests/unit/test_architecture.py` (including facade imports), Pyright, Go core
architecture checks, and benchmark Go compilation. It also checks proto drift
when Buf is installed. The AST checks always run with Tach's pytest impact
plugin disabled; this gate installs the `test` extra through `uv run --extra test`.
To run just those checks:

```bash
uv run --extra test python -m pytest -q -p no:tach tests/unit/test_architecture.py
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
The gate also deploys a synthetic workload at commit A, publishes a `.sdo/`-only
commit B, resets the configuration baseline for maintenance, and checks that
accepted manifests still use the deployed regular and init-container images.
Both annotated deployments and legacy deployments bound to the configured
deployed commit are covered. A later Service targetPort change must produce unexplained drift. Separate
coverage checks retain Service adoption and responder admission when another
workload cannot render, with the render gap recorded for retry; application
source changes must never reuse the old deployment images.
The gate also runs `TestHermeticNewDetectorInputs`: the real controller and
informer cache read the shared fastloop HTTP fake, collect per-node kubelet
summary/cAdvisor data, and consume real MongoDB handshake results through an
injected wire peer. A healthy baseline is quiet; StatefulSet readiness, memory
forecast and MongoDB protocol failures open one incident with numerical and
protocol evidence, and recovered inputs close it. Its test log includes wall
time through fixture cleanup. `TestHermeticProtocolDialer` checks healthy,
refused and malformed replies for all seven protocols. Fake API contract tests
cover list/watch for every added kind and Secret redaction on creation and
patch. The fake's subresource dispatch leaves room for independent pod log
support without implementing the logs input.

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
memory edge and broker in a temporary repository. Responder and reflection stages
run the real workers through the agent-script stand-in. Live recordings are converted
to scripts during export; archived recordings are converted in temporary storage.
Commands execute against captured state, and file edits run before validation.
Missing turns fail without a model call. There is one replay path and no live fallback.
Prompt and rollout readers remain analysis tools, including `reflection_prompt_replay`;
they do not execute agents. See the fastloop README for layout and commands.

Use `scripts/run_tests.sh` for the repository-wide suite.

Pytest and `scripts/test_python.sh` use six workers by default; `pytest-cov` combines the per-worker coverage so the 70% `fail_under` gate still sees the whole suite. Set `SDO_PYTEST_WORKERS=0` for an in-process run through the script, or pass `-n 0` to pytest, when iterating on a single test. Generated detector compilation is covered by the Go builder and broker suites.

Go-to-Python contract shapes are pinned by fixtures under `tests/fixtures/sdo/contracts/go/` (`sdo/operator/runtime/contract_golden_test.go`) and Python's strict models parse them (`tests/unit/sdo/contracts/test_go_contract_fixtures.py`). After an intentional Go encoding change, regenerate them with `go test . -run TestGoContractFixtures -update-go-contract-fixtures` in `sdo/operator/runtime` and rerun the Python tests. `TestContractShapesNeverEncodeNullCollections` fails on any `null` in those shapes: Python rejects `null` for a list field.

Agent turns are tested with agentshim's `FakeExecutor` through `tests/structured_turns.py`, which replays each provider's real stream format. Tests marked `live_agents` run the real Codex and Claude Code CLIs and are opt-in; cheap models keep them inexpensive:

```bash
SDO_RUN_LIVE_AGENTS=1 SDO_LIVE_CODEX_MODEL=gpt-6-luna SDO_LIVE_CLAUDE_MODEL=haiku \
  uv run pytest -m live_agents tests/
```

Live Codex or Kubernetes tests may require explicit markers, credentials, images, a cluster, and longer timeouts; do not infer production readiness from skipped external tests.

## Test the lifecycle contracts

### Lifecycle separation

Use backend test doubles to prove that source deployment and each health-judge attempt receive only their declared inputs. Verify fresh session identifiers, bounded correction feedback, source commit attribution, and independent acceptance.

### Operational-memory ownership

Create temporary Git repositories with `.sdo/` fixtures. Test:

- optional human-owned free text in `goal.md`;
- Go upkeep ownership and deterministic manifest inventory in `arch.md`;
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

## Go core architecture

Run `bash scripts/check_go_core.sh` to enforce the [functional-core boundary](go-purity.md).
It is included in the existing static-analysis and architecture gates. It installs
and caches golangci-lint 2.11.4, runs only depguard/forbidigo on declared core
packages, and checks structural purity plus application-owned transitive imports.

`bash scripts/check_go_mains.sh` (run by `check_errors.sh`) vets every Go program under
`benchmarks/` against the current operator API, so a runtime change that breaks one
fails static analysis in seconds instead of surfacing in the Python unit job.
The package registry lives in `scripts/go_pure_packages.txt`; negative fixtures
prove the linter rules are active.

Native receipt decisions have a hermetic Go suite in
`benchmarks/sregym/protocol/native_receipt_go`. Its fixtures are hand-authored
generated messages. Run `go test -run '^$' -fuzz '^FuzzNativeReceiptBoundary$'
-fuzzminimizetime=1s -fuzztime=30s -parallel=2 -timeout=2m .` there to attack malformed wire values,
round-trip acceptance, and rejection followed by a valid incident. PR CI includes this module and its fuzz seeds; Nightly CI runs the bounded fuzz campaign. Python
receipt tests exercise the real checker; transport-error tests mock subprocesses.
Submission phase tests live in `protocol/submission_go` (benchmark module) and
`tests/unit/benchmarks/sregym/adapter/test_submission.py`. The Go fuzz target
`FuzzSubmissionPhaseBoundary` checks that a POST requires matching observed phase,
that completion polls never post, and that malformed facts yield typed failures.
Run it with `-fuzztime=30s -fuzzminimizetime=1s -parallel=2 -timeout=2m`; Nightly CI runs the same bounded target, with its seeds retained on PRs. Image scope tests verify
separate Go compilation and binary-only copying into the benchmark worker.
Read-only controller observations are tested in `outcome_derivation` and
`operatorcli/controller_observation_test.go`. `FuzzControllerObservationBoundary`
checks malformed snapshots, closed lifecycle enums, incident correlation and a
rejected snapshot followed by a good one. Run it with `-fuzztime=30s
-fuzzminimizetime=1s -parallel=2 -timeout=2m`; Nightly CI runs the bounded target, with its seeds retained on PRs. Fake controller bookkeeping stays outside the generated snapshot shape.

## Abort and quiescence

Start with the stop/abort contracts and deterministic lifecycle cuts:

```bash
(cd sdo/operator && go test ./install ./operatorcli -run 'TestStop|TestParseStop' -count=1)
uv run pytest tests/unit/benchmarks/sregym/adapter/test_abort.py \
  tests/unit/benchmarks/sregym/adapter/test_processes.py \
  tests/unit/benchmarks/sregym/adapter/test_detached.py \
  tests/unit/benchmarks/sregym/adapter/test_native_persistent_ops.py
```

`TestStopSeededLifecycleCuts` runs 20 seeds across install, incident, responder,
reflection and drain cuts, including orphan Pods after Job deletion. The process
schedule kills a real launcher at each cut and checks zero registered live
helpers plus fake-cluster quiescence. SIGTERM/SIGINT tests exercise the abort trap.
Stuck-deletion contracts require an error and an inventory of remaining objects.

For level 6, select a disposable kind lane and its shared lock explicitly:

```bash
export SDO_ABORT_SMOKE_CLUSTER=<disposable-kind-lane>
export SDO_ABORT_SMOKE_RUN_DIR=<absolute-evidence-directory>
export SDO_ABORT_SMOKE_LOCK=<absolute-shared-lock-file>
bash scripts/smoke_sdo_abort_kind.sh
```

The script holds `flock` throughout, uses prebuilt controller and worker images
(overridable with `SDO_ABORT_SMOKE_BASE_IMAGE` and `SDO_ABORT_SMOKE_WORKER_IMAGE`)
and `sdo-toy-app:v1`, and installs the current controller binary/source. It opens
an actual toy incident with sleeping scripted workers, SIGKILLs the launcher,
then invokes abort twice and requires zero runtime objects and helpers while
preserving repository storage. The keeper and host bridge are inert stand-ins;
no credentials are read and no model is invoked. The control namespace and PVC
remain as preservation evidence; the script refuses to adopt existing fixture
namespaces. Inspect `result.json` in the evidence directory.

Normal launches through `scripts/run_sregym.sh` print their one-command abort:

```bash
uv run python -m benchmarks.sregym.adapter.persistent abort --state <printed-state-path>
```

For additional host helpers, set `SDO_RUN_HELPERS_FILE` to a JSON map such as
`{"bridge": ["python3", "bridge.py"], "credential-keeper": ["keeper", "--foreground"]}`.
The lifetime owner starts and registers these foreground commands before the
runner; do not start helpers in an unrelated terminal or daemonize them.
Aborting returns nonzero for surviving objects/helpers or failed verification.
A reused PID never authorizes a signal. Historical runs without a helper
registry require separate ownership evidence for any unregistered processes.
Graceful `teardown --state ...` still drains before calling the production stop
step. `sdo-operator stop --control-namespace ... --inspect` inventories production
runtime without modifying it; stop leaves memory, Secrets and ConfigMaps intact.

### Detached launches

Start long jobs through the detached entry point from the frozen worktree. Use
one absolute record path shared by every agent/worktree using the same lane,
and a unique run directory outside the repository for each launch:

```bash
uv run python -m benchmarks.sregym.adapter.detached launch \
  --record /absolute/shared/lane/launch.json \
  --run-dir /absolute/results/run-id \
  -- uv run python -m benchmarks.sregym.fastloop run <run-options>
```

For a normal benchmark, replace the command after `--` with
`uv run python -m benchmarks.sregym.run <config.toml>`. Optional `--helpers`
accepts the same helper JSON file as the foreground launcher;
`SDO_RUN_HELPERS_FILE` also remains supported. The lifetime owner registers and
starts these helpers. Keep secrets out of explicit command arguments.

The command returns after publishing the record and starting the owner in its
own session and process group. The record contains its PID, process group,
Linux start ticks, boot ID, UID, UTC launch time, state path, explicit worker
command and output paths. Output goes to `launcher.stdout.log` and
`launcher.stderr.log` in the run directory. Killing the invoking agent shell or
its `uv` wrapper does not stop the run. Deliberately sending SIGTERM/SIGINT to
the recorded owner still invokes the existing abort trap.

After a restart, find and adopt the live run instead of launching another:

```bash
uv run python -m benchmarks.sregym.adapter.detached status --record /absolute/shared/lane/launch.json
uv run python -m benchmarks.sregym.adapter.detached abort --record /absolute/shared/lane/launch.json
```

`status` prints the live record (exit 0), or exits 1 when it is no longer live.
Launching again with that lane record is refused while the owner or its
registered helpers remain live, even with a different run directory. Launch
and abort hold the same lane lock. Abort uses the recorded state and verifies
its registry identity, preserves the existing quiescence checks, and returns
nonzero for cleanup errors. Start ticks, boot ID and UID are checked before
signalling: a reused PID never authorizes a signal. Keep the record and logs
after completion; the next launch replaces a stale record. The printed
`persistent abort --state ...` command remains available as well.

### Fast image delivery

Use `uv run python -m benchmarks.sregym.fastloop preflight` before image builds.
Fastloop up repeats this gate before compiling or building. Mounts default to
this worktree; bare `--dev-mount` is accepted by up and reset. A code edit needs
only `uv run python -m benchmarks.sregym.fastloop reset --run-dir "$RUN_DIR" --dev-mount`.
Only dependency/tool changes need a new image tag via `scripts/build_sdo_images.sh`,
which uses the shared Bake graph and works with a private docker-container builder.

Up pushes through a lane-owned registry, preserving layers. An automatic archive
fallback warns on stderr and records `image_delivery: kind-load-fallback` plus
`image_delivery_reason` in the lane and incident rows. Run repeats that warning.
See the [fastloop procedure](../benchmarks/sregym/experiments/fastloop/README.md)
for ownership and cache details. Legacy live SREGym launches require
`--allow-non-fastloop`; use fastloop for normal experiments.

Fastloop image delivery regressions run without Docker or a cluster:

```bash
uv run pytest -q tests/unit/benchmarks/sregym/fastloop/test_fast_image_delivery.py tests/unit/benchmarks/sregym/fastloop/test_fastloop_loop.py
(cd sdo/operator && go test ./toolchain ./install ./operatorcli)
(cd sdo/operator/runtime && go test -run 'TestNativeStartupRejectsMissingToolchainBeforeKubernetes|TestNativeReflection|TestControllerConfig')
```

Direct controller launches require `--toolchain-image <controller-image>`;
`install-existing` supplies it automatically. The accepted memory host is
independent. Missing compiler configuration fails at startup.

CI checks out the immutable event commit (`github.sha`), so retargeting a PR
cannot make an in-flight job fetch a newer merge ref. Compare the actual
checkout SHA in job logs when two runs of the same PR head disagree: their
merge parents may differ even when the API reports the same current base.

The Python tests' native CLI is built by `scripts/operator_test_binary.py`.
Its cache filename uses the full SHA-256 of the Go-selected dependency source,
embedded files, module metadata, and effective toolchain/build settings. Each
reuse checks the binary's embedded `source-hash` against those inputs and fails
immediately on mismatch, including binaries without a stamp. This cache has no
prefix fallback. Go's separate compiler/module caches remain content-addressed
and can be shared across source revisions. To check an existing binary, run
`uv run python scripts/operator_test_binary.py --check /absolute/path/to/sdo-operator`.

## Hand-authored agent scripts (no model)

`benchmarks.sregym.agent_script` replaces the Codex executable beneath the real
workers. It accepts version 1 JSON or YAML, validates unknown fields and unsafe
file paths before executing anything, and emits Codex JSON events so structured
results and command auditing still use the normal provider parser. No production
module imports the stand-in. Roles are `source-deployment`, `health-judge`,
`responder`, and `reflector`; each result is the JSON object requested by that
role's model-turn schema (not the outer protobuf AgentResult).

```yaml
version: 1
turns:
  - role: reflector
    index: 0
    # incident: example-incident  # omit to match any incident
    steps:
      - kind: write
        path: .sdo/playbooks/example/README.md
        text: |
          ---
          schema_version: 1
          owner: responder
          fault_class: example
          originating_incident: example-incident
          ---
          Restore <AFFECTED_RESOURCE>.
      - kind: tool
        argv: [sdo-operator, memory, check]
    result:
      summary: Authored example playbook
      learning_decision: updated
      no_change_reason: null
      playbook_ids: [example]
      sdk_gaps: []
```

A `command` step has a shell `command`, expected `exit_code` (default 0), optional
`contains` assertion, `timeout` (up to 300 seconds), and bounded polling through
`attempts` and `interval`. `tool` takes an argv list beginning with `sdo` or
`sdo-operator`; shell metacharacters remain literal arguments. It also accepts a
`timeout` (default 60 seconds, up to 300), so full memory checks can keep their
original validation deadline. `write` takes a
relative `path`, UTF-8 `text`, and optional `executable`; `delete` takes a path;
`wait` takes `seconds` (at most 60). An `exit` step takes a nonzero `code`
(1–255) and ends the stand-in without a result, for crash/retry scenarios. The
turn is consumed before execution, so a later invocation selects the next turn.
All executed commands appear in the audit stream, including file edits and unsuccessful assertions. Bubblewrap must be
installed and user namespaces enabled: commands see a read-only host and may
write only the checkout and the responder tool exchange directory. As with real
workspace-write turns, a typed `sdo-operator memory check` runs outside that
sandbox. The broker remains the publication authority.

Turn indices start at zero **per role, incident and checkout**. Explicit incident
selectors take precedence over unspecified ones. Resumed responder turns and
reflection may use the same session ID. An exhausted script fails rather than
falling back to a model. Use a private `SDO_AGENT_SCRIPT_STATE` directory per
worker; `launch` creates one automatically. Results are literal and commands are
rerun: no recorded output is substituted for live evidence.

Before every Go or uv command on the shared host, source the cached environment:

```bash
source /mnt/data/shli/cache/env.sh
uv run python -m benchmarks.sregym.agent_script validate /absolute/script.yaml
uv run python -m benchmarks.sregym.agent_script convert --role responder \
  --out /absolute/editable-script.json /absolute/model_io/request-digest/*.json
```

Conversion reads successful Codex `model_recording` JSON files, orders them by
`turn_index`, extracts completed shell commands and the final structured JSON,
and rejects missing answers and failed recordings. Review paths and commands
before reuse; original absolute paths remain explicit and editable. For a turn
with Codex file-change events, supply `--file-edits /absolute/checkpoints/.../file_edits.json`;
the captured final UTF-8 writes/deletes are appended to the last turn. Without
those bytes conversion rejects file-change events rather than dropping edits.
Read-only turns cannot write the checkout; workspace-write turns also block
direct Go execution, matching the provider rules.

Tier 1 uses a local script and the ordinary typed worker task. Point KUBECONFIG at
the fake API used by the hermetic tests, and supply the normal worker task/result
configuration. The launcher installs a temporary `codex` wrapper and synthetic
login, selects the Codex provider, and executes the production worker:

```bash
SDO_AGENT_SCRIPT=/absolute/script.yaml \
  uv run python -m benchmarks.sregym.agent_script launch -- \
  python -m sdo.agents.health_judge.worker
uv run --extra test pytest -q -p no:tach tests/unit/benchmarks/sregym/test_agent_script.py
(cd sdo/operator && go test ./broker -run '^TestAgentScriptJudgeMemoryCheckAndBroker$' -count=1)
bash scripts/test_fastloop_hermetic.sh
```

The broker example authors a health detector, traffic generator/workload and
health-objective playbook through the actual judge worker, calls the real memory
checker, and publishes through the native broker. The generic parser tests cover
all roles, turn selection, resumed reflection, assertions and checkout escape.

Tier 2 uses the same script via `sdo-agent-script` ConfigMap (`script.json` key;
JSON or YAML contents). Build the benchmark image from an existing worker image
and generate registration manifests wrapping all four production worker entry
points. On an existing disposable toy lane:

```bash
docker build -f benchmarks/sregym/agent_script/Dockerfile \
  --build-arg BASE=sdo-responder:v0.1.0 -t sdo-agent-script:test .
kind load docker-image sdo-agent-script:test --name "$CLUSTER"
kubectl --kubeconfig "$KUBECONFIG" -n sdo-toy-sdo create configmap sdo-agent-script \
  --from-file=script.json=/absolute/script.yaml --dry-run=client -o yaml | \
  kubectl --kubeconfig "$KUBECONFIG" apply -f -
uv run python -m benchmarks.sregym.agent_script registrations \
  --out /absolute/script-registrations --image sdo-agent-script:test
# Existing controllers load registrations at startup. Publish and reset the lane
# before run, so a reused controller cannot silently retain legacy workers.
kubectl --kubeconfig "$KUBECONFIG" -n sdo-toy-sdo create configmap sdo-agent-registrations \
  --from-file=/absolute/script-registrations --dry-run=client -o yaml | \
  kubectl --kubeconfig "$KUBECONFIG" apply -f -
uv run python -m benchmarks.sregym.fastloop reset --run-dir "$TOY_RUN" --dev-mount
uv run python -m benchmarks.sregym.fastloop run --run-dir "$TOY_RUN" --agent sdo \
  --agent-registrations /absolute/script-registrations \
  --stream tests/fixtures/sregym/fastloop/toy_latent_repeat.json \
  --reflection-session fresh --diagnosis-grading off --no-checkpoints
```

The launcher reads the ConfigMap once at worker startup. Override its name with
`SDO_AGENT_SCRIPT_CONFIGMAP`, or use `SDO_AGENT_SCRIPT` for a mounted file. Assurance scenarios compile to the same version 1 contract through
`benchmarks.sregym.assurance.scripts.assurance_script`. The harness publishes
`script.json` plus a `warm.json` variant and a usage seed in `sdo-agent-script`.
The assurance CLI wrapper binds that bundle to the incident before executing
steps, records audited commands and deterministic synthetic token usage, and
selects the warm variant only when the real responder prompt surfaces validated
memory. It contains no fault or behavior menus.

The compiler supports both deny-all NetworkPolicy and missing ConfigMap faults.
`correct` diagnoses, repairs and polls live health; `wrong_then_correct` first
records an unsuccessful restart; `wrong_only_honest` reports failure;
`wrong_only_claimed` attributes a claimed success to the restart; `healed_noop`
only polls health and returns no action. Reflection `learn` writes final memory
and checks it, `no_change` returns an explicit reason, `crash_once` exits on its
first turn, and `crash_always` exits on every allowed turn. `invalid_then_learn`
returns a malformed first proposal for broker rejection and writes valid files
on the correction turn. Turn consumption persists across worker restarts and
isolated checkouts. Scripts retain pre-repair and pre-result chaos windows.
Qualification and composite oracles remain no-agent fault-driver checks; they
contain no Directive dependency and do not execute agent scripts.

Script string values reserve `{{lowercase_reference}}` for substitutions. No
expressions, filters or code evaluation are supported. Unknown references fail
script validation; references missing from the selected task fail before actions.
Go composite literals such as `{{APIVersion: "v1"}}` remain literal source.

Supported references are:

- `{{now}}`: worker UTC clock sampled at the start of this step (or when publishing
  the result). `{{turn.started_at}}` retains the turn's start time.
- `{{steps.0.started_at}}` / `{{steps.0.completed_at}}`: measured boundaries of a
  preceding step, or any existing step in the result. Forward references fail
  validation. Use these for action timing rather than literal timestamps.
- `{{incident_id}}`, `{{checkout}}`, and
  `{{request.field_name}}` for scalar typed request fields, using snake_case.
- `{{request.findings.0.primary_resource}}` and its `.api_version`, `.kind`,
  `.namespace`, `.name` fields; finding `.detector_id`, `.rule_id`, `.status`,
  `.severity`, `.summary`, `.evidence`, `.fingerprint`, `.playbook_ids.0`;
  `.parameter_bindings` or `.parameter_bindings.target_resource` and its resource
  fields. Binding names retain their original spelling.
- `{{request.surfaced_playbooks.0.playbook_id}}` and its `.parameter_bindings`
  (with the same binding/resource access as findings).

A reference occupying the entire value preserves its JSON type, so a responder
can use `resources: ["{{request.findings.0.primary_resource}}"]` and
`explained_detectors: ["{{request.findings.0.detector_id}}"]` in a root cause,
with `kind: detector-finding`, `source: "{{request.findings.0.detector_id}}"`
and `observation: "{{request.findings.0.evidence}}"` as evidence. The normal
native verifier still decides whether this diagnosis and mitigation earn credit.
Structured references embedded within text are rejected. Substituted file paths
are validated again. Quote inserted scalar values appropriately in shell commands;
use typed `tool` argv entries to avoid shell interpretation.


Captured repair reflection can be prepared as an ordinary version 1 script,
without editing the accepted checkout during generation:

```bash
source /mnt/data/shli/cache/env.sh
uv run python -m benchmarks.sregym.fastloop.reflection_script \
  --task /absolute/agent-task.json --capture /absolute/corpus \
  --out /absolute/results/reflection-script.json
SDO_AGENT_SCRIPT=/absolute/results/reflection-script.json \
  uv run python -m benchmarks.sregym.agent_script launch -- \
  python -m sdo.agents.responder.reflection_worker
```

The generated script preserves original captured bytes, incident provenance and
same-kind peer bindings, writes program/corpus files through audited steps, and
runs the shared memory validator. Reflection selects its incident-specific turn
from `request.outcome.incident_id`. Fastloop's generated registrations launch these scripts beneath the real
production workers. See the [worker guide](scripted-worker-retirement.md)
for task generation, writable exchange state and replay.

The responder half can also be generated without checkout side effects:

```bash
source /mnt/data/shli/cache/env.sh
uv run python -m benchmarks.sregym.fastloop.responder_script \
  --task /absolute/agent-task.json --out /absolute/results/responder-script.json
SDO_AGENT_SCRIPT=/absolute/results/responder-script.json \
  uv run python -m benchmarks.sregym.agent_script launch -- \
  python -m sdo.agents.responder.worker
```

Generate separately for each typed incident task. Four ordinary command steps
perform live planning, drift investigation/repair, fallback repair and verification
with attribution. Source reads and tool calls happen during execution, not during
generation. The checkout comes from the executing worker. Action and verification
timestamps come from the live clock. Transient state uses `SDO_TOOLS_DIR` and is
removed on success; a failed turn may leave its incident-scoped state for diagnosis.

A command may set `capture_json: true` to parse its successful stdout as one JSON
value. Subsequent steps and the result can reference `{{steps.0.output}}` or nested
fields such as `{{steps.0.output.status}}`. Whole-value references retain JSON types;
invalid JSON, missing fields and forward references fail. Output must contain only
JSON; diagnostics belong on stderr. This is live output, never recorded evidence.

`command.input_json` sends a JSON value on stdin. Use `input_json: "{{request}}"`
to pass the complete live task safely without interpolating it into shell syntax.

Fastloop toy lanes select `benchmarks/sregym/fastloop/agent_registrations` by
default. Their `generated_worker` launcher reads each typed task and generates
its responder or reflection script separately, then wraps the corresponding
production worker with the agent-script CLI. Scratch state uses the writable
tool request exchange; the accepted checkout is changed only by script steps.
Replay uses these generators with captured command transport. Custom registrations
remain subject to the live-run time-scale guard.

Broker compilation uses the running controller executable in a bounded subprocess.
`go test ./broker -run TestValidationSubprocessLimits` checks deadline and aggregate
process-group memory failures as infrastructure errors (exit 2). The CLI/broker
parity suite exercises this production subprocess transport. No validator image
or schema fingerprint is required; worker Jobs retain controller toolchain mounts.

Unit tests must synchronize on observable readiness or drive injected clocks.
`test_unit_tests_synchronize_without_wall_clock_sleeps` in the architecture gate
rejects `time.sleep` and `asyncio.sleep`, including imported aliases. Its only
allowance is the process-registry readiness helper: that sleep backs off between
explicit liveness checks. Go runtime deadline assertions use `testing/synctest`
so elapsed time measures the deadline contract rather than host scheduling. The
Go 1.24 SDK uses expired contexts and an explicitly driven clock. Kubernetes
Job tests subscribe to creation events instead of racing short polling windows.

To hunt scheduling and order dependence without changing the locked dependencies:

```bash
uv run --extra test --with pytest-randomly pytest -p no:tach -n 6 --randomly-seed=101 tests/unit
# Repeat with other seeds; concurrent copies supply CPU/build contention.
(cd sdo/operator/runtime && go test -race -count=5 -shuffle=on ./...)
```

The proto drift gate generates in a private temporary workspace and compares
those results with the source tree. It never replaces live generated files;
Python imports and Go compilers can run concurrently with the gate. The synthetic
`test_proto_drift_check_generates_outside_the_source_tree` regression exercises a
mismatching generator and requires the live source to remain unchanged.

Installer process tests remain in the integration tier established by #667;
both PR and nightly `scripts/test_go_ci.sh` use `-tags=integration`. Claim-age
and polling seam tests use fixed clocks and injected waits in the default tier.
The stop simulation continues using virtual time for production wait adapters.

### PR CI budget

PRs and merge groups run three jobs: `checks and Python`, `Go tests`, and
`Required PR CI`. The aggregate requires both validation jobs to succeed;
skips and cancellation fail closed. Static checks overlap Python tests within
one job. `check_errors.sh` already includes types, architecture and proto;
CI does not repeat those as separate jobs. Go modules use one temporary workspace and one `go test` invocation to schedule
packages across all modules, one compiler/module cache and normal package parallelism, with `-trimpath` allowing compiler entries
to survive generated-workspace path changes. No prepared-cache job or shard
matrix sits on the critical path. The Go job saves to its own `ci-tests-v2`
cache namespace on main: Actions caches are immutable, so sharing a save key with
Python checks let the first finisher discard the other job's generated builds.
The Go job also restores current-toolchain caches across dependency-hash changes;
Go validates compiler entries by content. Hash discovery can change after
submodule initialization, so an exact-hash-only fallback repeatedly restored
an obsolete cache on #664. Real compiler tests use the
`integration` tag in the same invocation (PR and nightly), including the separate
`builder/compiletest` package so the compile smoke overlaps builder validation.
The independent foreign-policy near-miss compiler test also runs in parallel. Both validation jobs have five-minute limits.

`Nightly CI` runs daily and through `workflow_dispatch`. Its Actions conclusion
reports failures. It retains the following extended checks:

| Nightly check | Why it is off the PR path | PR replacement |
| --- | --- | --- |
| All four controller/validator/responder/benchmark image builds, entry-point smoke and answer-key file inspection | Docker/tool installation and layer export dominated the previous image job | Python Dockerfile scope and image-delivery contracts; Go compilation |
| Go race instrumentation across all seven modules | Separate instrumented compiler graph and slower execution | All ordinary Go tests without race instrumentation |
| `TestReplay*` recorded incidents | Extended recorded regression tier | Ordinary controller, outcome and receipt contract tests |
| `TestChaos*`, `TestExhaustive*`, `TestExplorer*` | Extended interleaving/state-space campaigns | Ordinary deterministic controller tests |
| `test_scripted_reflector_writes_program_that_passes_real_memory_check[ordinary-repair]` | Repeated real memory-check/program compilation: 55.00 s on PR #657 | `test_reflection_script.py` capture provenance, no-change and timeout contracts; Go validator/broker acceptance tests |
| `test_scripted_reflector_writes_program_that_passes_real_memory_check[verified-drift-repair]` | Same compilation for the drift variant: 5.02 s after warming | `test_reflection_script_preserves_capture_provenance` and drift responder-script contracts |
| `test_respond_consumes_real_go_tool_responses` | Launches a Go integration test from Python: 11.73 s; Go CI already runs `TestHermeticResponderProgramReuseThroughRealToolTransport` | That same Go integration plus `test_responder_script.py` generated result/tool parsing contracts |
| Six 30-second fuzz mutation campaigns | Fixed mutation time adds minutes | Every checked-in fuzz seed executes in ordinary `go test` |

| Additional nightly test | Reason | PR replacement |
| --- | --- | --- |
| `TestHermeticRoutingCaptureCompilesReplayableMitigator` | Recompiles captured routing programs through the complete validator: 64.38 s on #664 | `TestRoutingCaptureWaitsForSlowHealthyProber` records/replays TCP evidence; capture count/time budgets; `TestTargetPortFixtureCandidateClearsAfterRepair`; `TestSimTrustedFastPathNeverRollsBackACorrectFix` |
| `TestBuildWorkspaceAcceptsValidMetricsDetectors` | Repeats successful controller/host compilation: 8.64 s on #664 | `TestBuildWorkspaceCompilesApplicationDetectors` plus `TestBuildWorkspaceRejectsInvalidMetricsDetectors`, and SDK metrics detector unit tests |
| `TestBuildMemoryHostUsesStagedMemory` | Repeated host compilation took 69.52 s on the first #666 run | `TestMemoryHostWorkspaceStagesAcceptedMemory` checks staging and host/controller isolation; `TestRemoteMemoryPreservesInputsAndCorpusReceipts` and memory-host simulation check real in-memory RPC parity |
| `TestHermeticSourceReconcileLearningAndTrustAtThreeSkipsResponder` | Repeated source-program compilation took 19.93 s on the first #666 run | `TestCreditEquivalentSourceRepairIgnoresServerDefaultsAndUnownedFields`, source render/reconcile contracts, deterministic trusted fast-path simulation and validator/broker tests |
| `TestCompileReportsTheFailingStage` | Vet/test fixtures each rebuilt the generated controller for ~51 s on the second #666 run before failing at the intended stage | `TestCompileStageDiagnostics` drives build/vet/test failures through the existing Go process seam and checks rule, path and line; real builder syntax, registration and invalid-metrics rejection still run |

`TestStopSeededLifecycleCuts` retains all 100 seed/cut cases but runs each in a
`testing/synctest` bubble: the installer's 100 ms deletion polls use virtual time
instead of accumulating 19 seconds of host time. No simulation seeds were cut.

The exact repeated end-to-end compile tests moved to nightly are listed in
`scripts/ci_nightly_go_tests.txt`. They cover assurance seed compilation,
CLI/broker parity across every fixture, duplicate CLI clean-tree/JSON and
scaffold compilations, toy-seed and isolated program validation, and repeated
builder program/host/baseline builds, plus compile-through-controller fastloop,
foreign-policy repeat and direct-files/learned-program CLI/broker integration. The assurance-seed test alone took
303 seconds locally. PRs retain `TestBuildWorkspaceCompilesApplicationDetectors`,
compiler rejection tests, validator rule tests, broker ownership/acceptance
contracts, CLI error/JSON diagnostics and SDK program unit tests. Nightly runs
the unfiltered suite with race instrumentation. Python's
`test_recorded_reflection_stage.py` also runs nightly: its real CLI/broker
and generated-program builds took 66–117 seconds per case locally. The removed
`test_scripted_worker_contract.py` was superseded by
`test_generated_worker_contract.py`; the three moved cases are listed above. Ordinary Python reflection, worker transport,
recording, provenance and Go broker contracts remain on PRs. Run `bash scripts/test_go_ci.sh` locally for the PR Go suite, or add
`nightly` for the full race suite. Both emit `go test -json` timing evidence;
Python CI logs include `--durations=30`. Keep full run logs outside the repo.


PR #657 timing investigation (Actions runs 41243599469 and 41243997276):
checks/Python took 241 s on the PR and 314 s before merge-group cancellation.
The Python suite took 190 s on the PR; the merge group was cancelled before
printing its durations table. Go cache restoration grew from 14 s to 69 s
(670 MB); uv sync took 2 s, pinned proto installation 8 s on a miss / 0 s on a
hit, and bubblewrap installation 5–7 s. Formatting plus overlapped static checks
and tests took 192 s / 217 s (cancelled). The generated-worker cases above
now run in the existing nightly integration step with bubblewrap installed.
The PR selection guard requires every ignored Python integration file to appear
in nightly, and keeps the cheap reflection/responder contracts on PRs.

## Writing a deterministic Go simulation test

Import `sdo.dev/operator/runtime/sim` from the runtime module or its subpackages.
Use `sim.Run(t, seed, func(t *testing.T, w *sim.World) { ... })` for a directed case,
or `sim.Sweep(t, body)` for seeds 1–256. Each body runs in a fresh synctest bubble.
Use `w.Now`/`w.Sleep` at injected clock seams; standard timers inside the bubble
also use virtual time. Keep cluster operations and fault consumption on the driver goroutine.
Use `synctest.Wait` to synchronize background work, never host sleeps.

Populate `w.Kube.Put` with synthetic unstructured objects. `w.Cache` supplies a
stale-capable SDK snapshot; `w.Client()` supplies the real client-go read client
through the in-memory transport for `core.KubernetesSnapshotProvider` and runtime
input observers. Set its `Clock: w.Now`. `w.Logs` holds timestamped bytes keyed by
namespace/pod/container/previous; `w.Kubelet` holds raw summary/cAdvisor samples.
Schedule `w.PodAfter`, `w.RolloutAfter` or `w.After` transitions and drive them with
`w.Advance` or the injected `w.Sleep`. Plain Go sleeps advance the bubble but do
not drain the driver's explicit cluster-event queue.

Set `w.Faults.Rules[edge]` for an ordered `[]sim.Fault`, or enable seeded
`RandomFailures`. Edges are `get`, `patch`, `snapshot`, `api`, `kubelet`, `logs`
and `probe/<protocol>`. Set `Partition[edge]` to retain an outage until cleared.
`DrawFaults(w.RNG)` configures bounded API churn and stale reads through
`Kube.SetFaults`/`Cache.SetFaults`. `Cache.CatchUp` models a completed relist.
`w.Dial` runs the existing seven link-protocol peers over in-memory connections.
`w.Endpoints` maps service addresses to protocols; inject `w.DialContext` into the prober.

Register `w.Invariant` hooks and call `w.Emit` at the actual decision/mutation
boundaries being tested; hooks do not infer controller decisions automatically.
Use the fast-path sweep as an example of a real mitigation path, and
`TestSimRealDetectionInputs` for production input collection.

Source the cached environment as described above before these commands:

```bash
(cd sdo/operator/runtime && go test ./sim ./prober . -run 'TestCrashLoopRecovery|TestSnapshotKindsAndRollout|TestSeededInvariants|TestFaultSchedule|TestHermeticProtocolDialer|TestSimRealDetectionInputs|TestSimTrusted' -count=20)
(cd sdo/operator/runtime && SDO_SIM_SEED=17 go test ./sim . -run 'TestSeededInvariants|TestSimTrusted' -count=20)
```

`SDO_SIM_RUNS` changes sweep length. The 45-second host-time budget is checked
between seeds; individual seeds must also terminate (use `go test -timeout`).
Tests that need processes or real sockets must be integration-tagged. Use the
[fidelity ledger](simulation-fidelity.md) to distinguish modeled edges from real
cluster coverage; the memory-host and full controller driver remain follow-up work.


Installer Git/archive coverage lives in `install/process_integration_test.go` and
`install/seed_sync_test.go`, both requiring `integration`. These retain the
accepted-commit, prepared-install RBAC, explicit kubeconfig and stale-file cleanup
checks. `scripts/test_go_ci.sh` passes `-tags=integration` in both PR and nightly
modes, so the move preserves their existing CI selection. The remaining installer
fake-client tests run without the tag and admit ready sync Pods synchronously;
the expired-claim test uses virtual time in a `testing/synctest` bubble.

## Go deterministic simulation policy

The Go codebase (operator modules and benchmark Go) uses deterministic tests by
default. New `_test.go` files must use injected clocks, seeded RNGs, `net.Pipe`,
in-memory gRPC, or `testing/synctest`, rather than wall clocks, real processes or
real sockets. The [Go nondeterminism ratchet](go-purity.md#codebase-nondeterminism-ratchet)
describes the APIs, exact production edges, limitations and shrink-only inventory.

```bash
bash scripts/check_go_determinism.sh
# After fixing existing violations; cannot add allowances:
bash scripts/check_go_determinism.sh --update
```

Real compiler/CLI/kind tests belong behind `//go:build integration`, with an
explicit `go test -tags integration` invocation in the CI job that owns them.
The initial ratchet rollout does **not** retag existing tests or change suite
selection: all existing real-compile, CLI, socket and kind violations remain on
the allowlist, so no coverage silently disappears. Before retagging a legacy
file, change the relevant CI invocation to retain its coverage. Tests newly
marked integration must likewise run in the owning integration job; tagging is
not a way to remove tests from CI. PR jobs retain their five-minute deadlines.

Memory Git-backed snapshot, inventory, ownership and validator checks require
`integration`; PR and nightly `scripts/test_go_ci.sh` already pass that tag.
Filesystem-only helpers and static checks remain available without it.

Deployment Git-backed registry, raw-strategy, health and BuildKit checks
require `integration` and retain their PR/nightly invocation. BuildKit fake
clients admit a completed Job synchronously or through an injected wait.
Command-observation tests inject commands and waits and run in the default tier.

Memory-host traffic contracts use virtual time and bufconn:

```bash
source /mnt/data/shli/cache/env.sh
(cd sdo/operator/runtime && go test . ./prober ./sim -run 'TestSimMemory|TestHostTrafficLifecycle' -count=20 -race -timeout=5m)
```

The traffic parity test uses two immutable revisions and checks stop/reset and
failed samples without OS sockets. It remains in the ordinary PR selection;
compiler and process coverage retains the integration tag and existing CI budget.

Core snapshot observation-time tests inject a fixed clock and compare the exact
observed timestamp. Core retains its Go 1.24 floor and uses no synctest; no tests
are integration-tagged for this migration.
SDK traffic tests use HTTP recorders, a fake transport and an injected clock,
including virtual request-deadline cancellation. The SDK retains Go 1.24 and
does not use synctest. Only the shell-based generated-reference check gains an
`integration` tag; both PR and nightly `scripts/test_go_ci.sh` run it with
`-tags=integration` within the existing five-minute PR budget.
Builder compiler/cache-seed process tests live in `native_integration_test.go`;
manifest-location and prohibited-import rejection tests remain untagged.
PR and nightly `scripts/test_go_ci.sh` already include `-tags=integration`,
including the existing `builder/compiletest` smoke package. Nightly-only
compilation selections and the five-minute PR deadlines are unchanged.
CLI evidence commands and Git-backed memory fixtures require `integration`.
The pure host parser and memory admission-error checks stay untagged.
`scripts/test_go_ci.sh` keeps these tests in their existing PR/nightly selections
with `-tags=integration`; no CI timeout or selection changes are needed.
Broker Git checkout/commit, CLI/compiler parity, agent-script and validator
process-limit tests require `integration`. They continue in
`scripts/test_go_ci.sh` with `-tags=integration` on PRs and nightly; the existing
nightly-only parity selection and five-minute PR budget are unchanged.

Runtime drift, ownership-adoption, read-view and usage observer tests use a
synchronous handler RoundTripper through client-go, preserving wire encoding
without real sockets. Cache observation timestamps accept an injected clock;
logs, metrics and usage retain their injected clocks with defaults owned by
`runtime/internal/lifecycleio`. This migration adds no integration tags.

Program and responder receipts share `PlanExecutor.Now`; candidate diagnosis
duration accepts `CandidateOptions.Clock`. Retained-drift evaluations use the
caller's evaluation time, and baseline diffs use their snapshot's observation
time. These clock seams remain in the default test tier.

Prober traffic, primitive-handler, link and reachability tests run with virtual
clock time, handler-backed HTTP and injected in-memory dial outcomes. Real
connection-pool/listener-closure contracts remain in
`prober/socket_integration_test.go`; PR and nightly `scripts/test_go_ci.sh`
already include `-tags=integration`, retaining both tests and the five-minute
PR budget.

Responder diagnosis settling and trusted fast-path settling share
`PlanExecutor.Sleep`; resource-version retry jitter accepts `PlanExecutor.Jitter`.
Tests can advance one simulation clock and seeded RNG through these seams.
Cancellation still prevents the post-settle diagnosis or apply.

Metrics observer and Prometheus adapter unit tests use injected handler-backed
HTTP and virtual deadlines, including a source that ignores cancellation.
The three real CLI metrics-URL wiring tests and default-client redirect wiring
run with `integration` in the existing PR/nightly `scripts/test_go_ci.sh`
invocations; unit fixtures do not claim real HTTP transport coverage.

Controller dispatch, persistence, admission and effect-durability tests use
virtual time and quiescence before negative dispatch assertions. Broker and
dispatch queue helpers synchronize by receiving and restoring the buffered
result, preserving controller ownership of consumption. Informer notification
helpers await the event directly instead of host-clock polling.

The real client-go informer/restart test sets a positive virtual resync interval.
Client-go's disabled-resync path selects on a package-global channel created
outside the synctest bubble, preventing virtual timers from advancing.
`KubernetesCacheConfig.ResyncPeriod` leaves production's disabled-resync default
unchanged while allowing that scheduling boundary to remain in-bubble. This
does not add client-go watch streams to the simulation world's model.

Maintenance, traffic, Service reachability, rollout and informer-observation
polling accept client-go clock interfaces. Lease timestamps and expiry callbacks
share an injected clock via `NewLeaseElectorWithClock`; the default constructor
keeps production time. An expiry callback retires its fired timer handle before
revoking authority, so it never tries to stop the timer currently firing.

`RuntimeOptions.Clock` drives controller wakeups, startup/evaluation timestamps,
observer clocks, lease renewal and executor waits. The metrics-URL integration
contract checks its clock through the emitted finding's `observed_at` metadata;
real HTTP wiring remains in the existing integration CI tier.

`NewResponderLoopWithOptions` accepts a replayable session-ID source while
validating its 32-character lowercase hex namespace. Session tool polling and
temporary reply IDs can be injected; collision tests retain exclusive file
creation and prove an existing reply temporary cannot be overwritten.

Closure health/state-diff and firing telemetry, lifecycle events, maintenance,
lease timing, responder settle/mitigator deadlines and cache write observation
run in synctest bubbles. Their positive timeout and mutex-release assertions
retain virtual deadlines; the fake informer uses a positive virtual resync.

Chaos replay controls are explicit test flags: from the runtime module, use
`go test . -run '^TestChaos' -chaos-seed=17 -timeout=5m`, or set
`-chaos-runs=128` for a fixed sweep. These replace `SDO_CHAOS_SEED` and
`SDO_CHAOS_RUNS`. Sweeps always run their configured seeds; they no longer
silently truncate coverage based on machine speed. Closure/reflection explorers
retain their state bounds, and campaign invocations (including nightly CI) use
`-timeout=5m` as the wall-clock cap.

Runtime accepted-Git, broker/CLI/compiler/Python-process and recorded worker
fixtures require `integration`; their shared pure detectors, read views and
non-process tests stay in the default suite. PR and nightly `test_go_ci.sh`
already include `-tags=integration`, with the existing nightly-only compiler
selection and five-minute PR budget unchanged. `split_go_integration.sh` reports
AST dependency candidates and moves explicitly selected declarations, preserving
embedded Go strings; review candidates and compile both build selections.

Cache observation-time contracts compare an exact injected timestamp. Dispatcher
Job waiting, fast-path restarts and worker completion run in virtual time, with
an explicit virtual restart boundary for fresh evidence. Process-only fastloop
wait helpers stay with their integration fixtures. Simulated protocol peers use
the world clock for safety deadlines; default peer behavior is unchanged.
