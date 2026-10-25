# Testing guide

SDO spans Python agents, Go operator modules, generated detector workspaces, and optional Kubernetes smoke tests. Validate the narrowest affected contract first, then expand in proportion to risk.

## Test organization

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

Reflection checks its exact returned decision and final checkout through the real memory
CLI before publishing a worker result. Failed checks resume the same provider session,
with at most three correction turns inside the original deadline. The real-CLI tests in
`tests/unit/sdo/agents/responder/test_reflection_final_check.py` cover both providers,
fresh and resumed sessions, changed bytes after an earlier passing check, withheld
invalid results, and validator process cleanup. Shared compile checks evaluate learned
detectors over complete corpus snapshots, including unlabelled locator bindings;
program and shared-library edits retain the healthy-baseline input. The broker parity
tests compare exact rejection diagnostics for missing coverage, inconclusive diagnoses,
missing source evidence, and a program-only edit against a wider healthy snapshot.


Historical plumbing regressions have focused checks in the same suites:

| Regression | Contract check |
| --- | --- |
| Restart live recheck | `TestResponderRestartRediagnosesWithoutUndoOrRestoredHandles` preserves fixes and budgets. `TestStateRoundTripRetainsAppliedActionsAndProgramEvidenceWithoutRollbackState` checks durable outcome evidence without rollback authority. |
| Physical `.sdo/` tool evidence | The responder loop test rejects physical paths in recorded tool calls; `TestSyntheticWorkerResultsThroughContracts` covers typed worker envelopes; responder validation rejects physical tool evidence. |
| Plan apply RBAC | `sdo/operator/install` tests check Service and Deployment get/patch grants. |
| Tool session restart permissions | The responder loop reparses real directory modes; `TestSessionToolServerIgnoresAtomicRequestTemporaryFiles` protects the CLI write protocol. |
| Capture size and timing | `sdo/operator/operatorcli` draft tests reject oversized captures; the controller checks that the `open@detect` capture never delays or blocks dispatch. |
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
The gate needs Go and `uv`, but no cluster or container runtime.

### Rendered deployment lock (LK1)

The fast hermetic gate includes rollout/restart lock contracts, the FX1 memory
publication regression using durable locks, and the seeded lock schedule.
`SDO_CHAOS_SEED=<n> go test -run '^TestSimRenderedLockSchedule$' -timeout=5m .`
from `sdo/operator/runtime` replays one schedule. The properties require unchanged
lock bytes after memory commits, recovery between rollout and lock publication,
and continued Service drift when another object's lock evidence is unavailable.

The scripted Level 6 uses only the task's lane `luna-pilot-53`:

```bash
source /mnt/data/shli/cache/env.sh
flock /absolute/path/to/shared/lane53.lock env \
  SDO_LK1_RUN_DIR=/absolute/path/outside/repository/lk1-e2e \
  bash scripts/test_rendered_lock_kind.sh
```

It creates that kind cluster if absent, loads `busybox:1.36`, and runs a real
Deployment with a mutable regular and init image. The production controller,
informer cache, SSA adoption, lock broker and `sdo source` tool process two
scripted targetPort incidents with a human memory publication and controller
restart between them. Each incident requires a restored real HTTP route and
unchanged lock bytes. It makes no model calls, deletes its test namespace, and
leaves lane 53 available for inspection. It does not install a controller Pod or
exercise model-authored workers; those remain separate deployment gates.

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
round-trip acceptance, and rejection followed by a valid incident. The existing
controller CI matrix includes this module and the bounded fuzz run. Python
receipt tests exercise the real checker; transport-error tests mock subprocesses.
Submission phase tests live in `protocol/submission_go` (benchmark module) and
`tests/unit/benchmarks/sregym/adapter/test_submission.py`. The Go fuzz target
`FuzzSubmissionPhaseBoundary` checks that a POST requires matching observed phase,
that completion polls never post, and that malformed facts yield typed failures.
Run it with `-fuzztime=30s -fuzzminimizetime=1s -parallel=2 -timeout=2m`; the existing
controller CI matrix runs the same bounded target. Image scope tests verify
separate Go compilation and binary-only copying into the benchmark worker.
Read-only controller observations are tested in `outcome_derivation` and
`operatorcli/controller_observation_test.go`. `FuzzControllerObservationBoundary`
checks malformed snapshots, closed lifecycle enums, incident correlation and a
rejected snapshot followed by a good one. Run it with `-fuzztime=30s
-fuzzminimizetime=1s -parallel=2 -timeout=2m`; the operator CI leg runs the bounded
target. Fake controller bookkeeping stays outside the generated snapshot shape.

Uncontrolled text boundaries are fuzzed where the text is hand-written or model
written. `memory.FuzzLegacyOutcomeStream` (retired `outcomes.jsonl`) and
`sdk.FuzzObjectRefParseFormat` run their hand-written seeds in every `go test`;
run `go test -run '^$' -fuzz '^FuzzLegacyOutcomeStream$' -fuzztime=180s` (cap 5
minutes) in `sdo/operator/memory` to explore further. The Go and Python object-ref
parsers share `tests/fixtures/sdo/contracts/object_ref_grammar.json`, and Hypothesis
covers `sdo act` receipt recording in `test_recorded_actions.py`.

Proxy latency acceptance runs in the operator CI job without race instrumentation:
`GOMAXPROCS=2 go test -p 1 -count=3 -run '^(TestProxyWarmAddedP95|TestLargeResponseWarmAddedP95|TestProjectionTailLatency)$' -v ./kubeproxy ./operatorcli`.
The `!race` forwarding tests use 50 warmup pairs and 200 measured pairs, balanced direct/proxy
ordering, and 1 ms pacing outside the measured pair. The acceptance threshold is
10 ms for the nearest-rank paired added p95. Redaction, exact forwarding, bounded
admission and gap assertions continue to run under `-race`.

Record-admission latency also uses 50 warmups and 200 paced normal-build samples, with a 10 ms p95 limit. Its metadata-only omission, bounded admission and dropped/incomplete checks remain in the race suite.

SREGym discovery and worker contract tests use the pinned harness and its application
charts without Kubernetes or model calls. Before running them in a fresh checkout,
initialize both repository edges:

```bash
git submodule update --init third_party/sregym
git -C third_party/sregym submodule update --init SREGym-applications
git -C third_party/sregym/SREGym-applications submodule update --init astronomy-shop
```

The Python unit-test CI job initializes these application sources explicitly so
the discovery gate runs instead of silently skipping its cross-repository checks.

### Health policy refresh after source advances

The native builder refreshes stale health requirements through the registered judge
and broker before compiling detectors. Accepted judge commits bind policy to all
tracked application inputs outside operational memory, the human objective and
health detector revision. Memory-only outcomes, playbooks and rendered locks do
not trigger judge work. The runtime refuses stale evaluation, dispatch, plan
writes and closure, and requests a builder relaunch for adoption. A refresh failure
leaves health unverified. An investigation or frozen closure that predates a
source, objective or health detector change is durably archived as a visible
failure after adoption, allowing
fresh evaluation without publishing or acknowledging its obsolete success.

The model-free tracing regression and seeded restart schedule run with:

```bash
(cd sdo/operator/runtime && go test . -run 'TestHealthRefresh|TestTracingSwapRefresh|TestSimHealthRefresh|TestSimRV6HealthRefreshCrashSchedules' -count=1 -timeout=5m)
```

The tracing capture replaces Jaeger with a collector while preserving old
architecture, compiles accepted detectors, and requires collector coverage without
Jaeger. The 20-seed RV6 schedule sends real SIGKILLs at proposal, request, Job, result,
before/after acceptance, before/after adoption, and before/after stale-closure
failure persistence. It retains fake Kubernetes resources and controller state
across processes, optionally advances memory while interrupted, and requires
fresh policy acceptance, durable stale-evidence failure, and fresh evaluation
after another restart. Accepted work is reused and the old binary remains
unauthorized. The smaller cancellation schedule checks handled worker errors.
These checks use scripted judge results and no cluster or model calls; they do
not evaluate actual judge model behavior.

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
The expiry guard archives abandoned requests
with typed `AGENT_TIMEOUT` evidence and requires fresh observations for a new
incident. These hermetic regressions run with:

```bash
(cd sdo/operator/runtime && go test -race . ./lifecyclecore -run 'TestExpiredIncident|TestResponseExpiry|TestLearningTrustUsesAccepted|TestMissingOptionalLearning|TestMissingLearningCommit|TestPreparedResponderRequestSurvives|TestLearningEvidenceGitError' -count=1 -timeout=5m)
```

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

### PR CI execution budget

PR CI runs at most **three work jobs** in parallel: **Python/static/proto and Go modules**,
**All runtime race tests**, and **Affected controller images**. The fourth job,
`Required PR CI`, runs only after all three complete and remains the single
branch-protection check. There is no selector job or test matrix. Repository
policy rejects a fourth work job, counting every matrix expansion, as well as
missing or greater-than-ten-minute job timeouts and bypassed enforcement.

The budget is **600 seconds of execution along the longest `needs` chain**.
`scripts/check_ci_budget.py` reads Actions `started_at` / `completed_at` timestamps,
including setup and cache teardown, and adds dependent durations. Queues never
count; parallel siblings use their maximum, rather than their sum. The report
names the critical path and five slowest jobs. Missing API evidence fails closed.
Because the required check is still running when it measures, it reserves its
entire one-minute job timeout for its own execution and cleanup. This conservative
upper bound enforces the complete path; aim for at most eight minutes of execution
to retain headroom. All three work jobs time out after nine minutes.

The Go module job starts its longest module alongside a sequential lane for
smaller modules. The full Python suite overlaps those lanes with one pytest worker
and one Go compiler; both lane results are required even when one fails. Static
and proto checks run first on that runner. Runtime
uses **two disjoint partitions on one runner**, `contracts` and `integration`,
without competing with unrelated module compilation. Run both locally with
`uv run python scripts/test_runtime_shard.py --shard all`, or select either
partition by name. Inventory discovery runs once before both partitions start,
with shared Go build caches; both exit statuses are collected even if one fails. Every package,
example, fuzz seed, replay, chaos and explorer test remains in the race suite.
The exactly-once guard rejects missing or overlapping names; all nested scenarios
and seeds remain unchanged. The unchanged 83 MiB restart fixtures stay on PRs.
The partitions also retain the full trust property matrix. Immutable health-policy
checks compile one binary per accepted policy revision and execute each observation
against it; simulation pumps wait for completion events instead of scheduler ticks.
Probe timeouts use Go's virtual clock while preserving their error/replay assertions.
The Gate 1 names and `scripts/test_fastloop_hermetic.sh` selectors stay intact.
Go emits `-json` timings for profiling; aggregate top-level `pass` event `Elapsed`
values rather than adding nested subtest timings a second time.

`nightly.yml` runs daily at 05:17 UTC and supports manual dispatch. It runs all six
existing 30-second boundary fuzz campaigns, three normal-build proxy latency
acceptance repeats (including the large-response distribution), and builds/probes
all four images even when no image input changed. These checks need sustained
mutation/repetition or full build/export work. PRs retain all fuzz seeds under
`-race`, one-second active fuzz smoke, one repeat of all three normal-build latency tests (including the large-response
check with its unresolved intermittent failure),
and deterministic forwarding/redaction contracts. Every affected image is still
built, smoked, probed and checked for forbidden benchmark files on PRs.

Static analysis already includes type and architecture checks, so their standalone
jobs were redundant. They now run once on the Go/Python runner alongside formatting
and proto lint/codegen drift. Nightly publishes one maximal shared image-layer
cache; PR image jobs restore it without repeating the large compiler/toolchain
layer upload (175 seconds in a hosted sample). Every affected image still builds
and receives the same smoke, probe and forbidden-path audit.
Image setup reads the committed native SDK pin,
which the Go static gate and built-image validation independently verify, avoiding
a second full controller build just to obtain its schema string.

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
`install-existing` supplies it automatically. The traffic prober image is
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
