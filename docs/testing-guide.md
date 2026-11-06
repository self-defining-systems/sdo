# Testing guide

SDO spans Python agents, Go operator modules, generated detector workspaces, and optional Kubernetes smoke tests. Validate the narrowest affected contract first, then expand in proportion to risk.

## What we test

Prove each guarantee at the cheapest level that can hold it (the `design-for-verification` order).

**Who owns what.** Controller properties are tested. Agent-written code (detectors, programs) is checked at the gate: `sdo-operator memory check`, the same check the broker runs, must reject bad code. The gate table (`memory/validate/gate_table_test.go`) holds one row per validator rule: a bad change that must be rejected for that rule, plus good changes that must pass. `TestGateTableNamesEveryRule` fails when a rule has neither a row nor a stated reason. The shell code path (`shell.format`) has its own `shell:` rows. The broker's `memory.conflict` cannot be a row (it needs two proposals on one repository); `broker/memory_conflict_integration_test.go` covers it. Compile rows run on FakeGo on every PR and on the real `go` under the `integration` tag (`gate_real_integration_test.go`); both must produce the same rejection. Agent behaviour (repairing a fault, saving a useful program) is measured in live runs and never unit-tested.

**By construction.** These hold through the architecture and are guarded by static checks, not runtime tests:

- Only the controller writes to the cluster: programs return plan values, and the SDK read views have no write methods.
- Agent code cannot reach the network or the cluster: the memory check enforces an import allowlist.
- Accepted memory comes only from broker commits.

Accepted choice: the memory host holds write-capable Kubernetes credentials and pod exec. The host is a small amount of SDO's own trusted code, and agent code is the boundary the import allowlist enforces.

**Behavioral properties.** For now we check two things, both on one seeded simulation of the real controller loop, with simulated Kubernetes, adversarial agent and program stand-ins, and crashes at effect boundaries (precise definitions are the doc comments in `sdo/operator/runtime/sim/properties.go`):

1. The controller is reliable. Under bounded faults (controller and host crashes and restarts, API errors and stale reads, agents and programs that crash, hang or return garbage) it never wedges: every incident reaches one terminal outcome after faults stop, and a failed incident never blocks a later one.
2. The controller manages detectors well. Every detector in the active memory revision is evaluated within a few times its own configured interval, through memory swaps, closure and agent work. A detector that errors, panics or hangs does not delay the others. A newly activated revision's detectors start and removed ones stop, with no controller restart.

`TestHarness` (`sdo/operator/runtime/harness_*_integration_test.go`) checks both on every run; its limits are in [simulation-fidelity.md](simulation-fidelity.md). Replay a failing seed with the command the failure prints.

Other behavioral properties (fixed means verified, writes happen once, nothing is lost, saved memory gets used) are not checked now.

**Honest fakes.** Conformance tests compare the simulated Kubernetes API (`TestKubeConformance`) and fake Go (the compile rows of the gate table, below) with the real ones. There is no fake git: tests run the real binary through `internal/testutil`, and `TestGitHelpersObserveWhatPlainGitObserves` checks the helpers add nothing a test observes. a real run that finds a bug the simulation missed is a fidelity gap ([align-simulation](../.agents/skills/align-simulation/SKILL.md), [ledger](simulation-fidelity.md)). The controller end-to-end run (below) is the process-level check without a cluster; the scripted toy run on kind is the one end-to-end check with real nodes and images.

A test that maps to none of these is a candidate for deletion.

## Real API server for integration tests

`sdo.dev/operator/runtime/envtestio` (`integration` tag) starts a real kube-apiserver and etcd at the kind lane's
Kubernetes version and returns a client configuration and a kubeconfig file:
`env := envtestio.Start(t)`, then `env.Client(t)`, `env.Kubeconfig`, `env.CreateNamespace(t, "app")`. The first use
downloads the binaries into `$SDO_ENVTEST_CACHE` (default the user cache dir; on the lane hosts export
`SDO_ENVTEST_CACHE=/mnt/data/shli/cache/envtest`), or set `KUBEBUILDER_ASSETS` to an existing directory. There is no
controller manager, scheduler or kubelet: nothing reconciles, Pods stay Pending, and namespaces never finish
terminating. PR and nightly CI cache the binaries in `~/.cache/sdo-envtest`, keyed by the Kubernetes version
(bump the key with `envtestio.KubernetesVersion`); a cache miss runs `scripts/install_envtest.sh` once (pinned controller-tools release, SHA-512 checked, retried) and CI exports `KUBEBUILDER_ASSETS`; tests do not download in CI. Locally the same script, or the first test, fills the cache. Tests that start it belong in
`if testing.Short() { t.Skip(...) }` (PR CI passes `-short`; nightly does not) unless they stay well under PR CI's budget.

## Test organization

- `tests/unit/sdo/` — agent and contract behavior.
- `tests/unit/benchmarks/sregym/adapter/` — benchmark-adapter transport and receipt behavior.
- `tests/unit/libs/` — shared-library contracts.
- `tests/integration/` — cross-package and external-process behavior.
- `sdo/operator/sdk/inputs/carrier_test.go` — AST inventory of SDK input sources. The SDK's behaviour is covered by the shipped detectors' own tests, which run at the gate.
- `sdo/operator/core/generic_capture_guard_test.go` — mutable kinds cannot enter the capture/replay path unguarded.
- `sdo/operator/runtime/*_test.go` — the controller harness (`TestHarness`), contract and proto parity, and source-scanning ratchets; the remaining `*_support_test.go` files hold helpers those tests need.
- `sdo/operator/memory/validate/skills_test.go` — skill drift: rule citations and examples. `scripts/check_go_errors.sh` runs `gen_skill_references.sh --check` and `gen_kube_read_clients.sh --check` for generated-file drift (`bash scripts/gen_skill_references.sh` regenerates the reference); no Go test repeats those checks.
- `sdo/operator/{broker,memory,install,lifecycle}/*_test.go` — accepted state and lifecycle ownership.

For a fresh checkout or worktree, run `uv run python scripts/init_test_prereqs.py` once to initialize SREGym and every nested application/chart at the recorded SHA.

## Seeded simulation sweeps

Tests whose subject is not compilation use `sim.Compiler` as the broker's compile hook instead of
`validate.CompileCandidate`; keep real compilation in the builder, validator and CLI tests.

## Kubernetes conformance

`TestKubeConformance` (`sdo/operator/runtime/sim`, `integration` tag) runs one table of scenarios against a pinned
kube-apiserver (`envtestio`, above) and the simulated API and compares what a client sees (about 17 s, runs on every PR); see
[simulation fidelity](simulation-fidelity.md#kubernetes-conformance). When the simulation must behave like the server
in a new way, add a scenario first.

## Git-backed Go fixtures

Tests initialize temporary repositories with `internal/testutil.InitGit(t, root)`.
The helper disables automatic GC/maintenance and detachment in repository-local
configuration, which also protects production Git calls and linked worktrees.
`testutil.Git` runs commands synchronously with a bounded context; callers cannot
start a command without waiting. The production broker also keeps automatic
maintenance in the foreground for its Git commands and persists this policy in
its independent proposal/staging clones; Git clone does not copy local config. Keep goroutines using a repository joined before
test cleanup.

Use these helpers rather than a bare `git init` in a test.

## Synthetic Go edge

A generated-workspace check costs 14–24 s of real compiling and linking (controller link 9 s,
two test-binary links about 5 s each). Contracts about *which stage runs, which stage
failure rejects, and how a failure is reported* do not need that: install
`internal/testutil.FakeGo(t, rules...)`, a `go` on PATH whose stages pass unless a
`GoRule` fails them (by verb, by package pattern covering a directory, by a marker file).
It logs each stage, leaves an empty executable for `build -o X`, and works through child
processes such as the operator CLI. Build any real binary before installing it.
The gate table's compile rows are the FakeGo-versus-real-Go comparison: each row fails one stage on FakeGo
and fails the same stage on the real toolchain with an edited fixture, and both must report the same rule at the same path.
A test that asserts compiler or replay *output* (replay notes, healthy-baseline refusals,
cache identity, link flags) needs the real compiler and the `integration` tag.
`Workspace.Check` links the generated test binary once: `go test -json ./generated` is
classified per test (only a failing `TestProgramCorpus` is advisory), and the
informational pass runs every other top-level directory. A result that cannot be
attributed to one test (build error, panic, timeout) is rerun without the corpus so the
rejection keeps its usual diagnostics.
Use `Workspace.Check` rather than `ValidateAndBuild` unless the controller binary is the
subject: `Check` runs the same acceptance and advisory stages without the controller link.

## Waiting in Go tests

`Execute` polls until its context ends, so running it under a fixed short
deadline (`context.WithTimeout(..., 100*time.Millisecond)`) races it against the
scheduler; the race detector under host load flaked on this. Wait for the
condition the test asserts instead. In `worker_registry`, `executeUntil` runs
`Execute` and cancels it once the condition holds, with a 30 s failure ceiling.

Counted sleep loops in Go and Python tests also race loaded CI when their literal
sleep budget is below five seconds. The Python `tests/unit/test_polling_policy.py`
AST check runs in `check_errors.sh` with negative controls. Wait for the asserted condition with a generous failure ceiling
(or a notification). Intentional timing loops can carry a comment on the loop or
the preceding line: `test-polling: allow -- <reason>`. Nested loops own their
waiting budgets; paced event generation is not a polling deadline.

## SREGym pin ancestry

`scripts/check_submodule_pin.py` (a `check_errors.sh` step) fails when the
`third_party/sregym` gitlink is not equal to, or a descendant of, the base
branch's pin. A merge that keeps the branch's older pin otherwise reverts the
base silently. The base is `--base`, else `origin/$GITHUB_BASE_REF` (fetched
with depth 1 in a shallow CI checkout), else a default branch already merged
into HEAD. Ancestry comes from the initialized submodule when it has both
commits, else from a blobless fetch of the two pins into a temporary
repository. It skips with a message when the base or its pin is unavailable.

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
`scripts/test_go_ci.sh` select that tag; `scripts/run_tests.sh` also retains it so its
full selection continues to execute process tests. The benchmark replay module is
included in the full runner and both CI modes.

`scripts/check_errors.sh` runs Ruff, Tach, the AST import and ownership checks in
`tests/unit/test_architecture.py` (including facade imports), Pyright, Go core
architecture checks, and benchmark Go compilation. It also checks proto drift
when Buf is installed. The AST checks always run with Tach's pytest impact
plugin disabled; this gate installs the `test` extra through `uv run --extra test`.
The architecture gate also resolves every tracked `from benchmarks.sregym.adapter import ...`
statement through the real facade, with a missing-export negative control. This catches
missing lazy exports before worker startup; test doubles must stay scoped to their owning module.
To run just those checks:

```bash
uv run --extra test python -m pytest -q -p no:tach tests/unit/test_architecture.py
```

`check_errors.sh` also calls `scripts/check_contracts_fast.sh` (about 5 s warm,
15 s cold). It runs the cross-language parity tests that used to fail only in
hosted CI: every test in the three small Go contract modules, the runtime tests
named `*MatchProto`, `*MapsToProto`, `Proto*`, `TrafficProto*`, `*ClosedEnums`
or `*MatchesGenerated*`, and `tests/unit/sdo/contracts`. Name a new parity test
to fit one of these patterns, or put it in one of these places, so the local
gate picks it up. For example, adding an outcome classification to the proto
without listing its Go constant fails `TestClassificationsMatchProto` here.

In hosted CI the checks job runs `check_errors.sh --python-suite`, which skips the
Go parity tests (the Go tests job runs them), the fast Python ratchets and the
Python contract tests (the full Python suite contains them), and compiles the
operator with `go list -export` instead of linking every command.

The responder command guide is generated from the production CLI definitions and
checked in seconds by `scripts/check_errors.sh`. After changing the command surface,
run `uv run python -m sdo.agents.responder.command_reference` and commit the updated
`sdo/skills/sdo-incident-response/references/commands.md`. The focused contract tests
are in `tests/unit/sdo/agents/responder/test_command_reference.py`; they include a
missing/stale-reference negative control. The responder image's cached model-tool
layer installs `jq` and runs a JSON extraction smoke check during the build.

The model-bearing image build runs `scripts/check_model_cli_versions.py` against
`claude --version` and `codex --version`. Default model compatibility requirements
live in `sdo/operator/model_cli_requirements.json`; update the Claude CLI minimum
alongside the default model list when a model requires a newer CLI. A mismatch
fails the build before any model call. The image freezes both measured versions
and the requirements in `/opt/sdo/model-cli-versions.json` and exposes the verified
pins as `dev.sdo.claude-code.version` and `dev.sdo.codex.version` image labels.
The focused no-Docker tests are `tests/unit/scripts/test_model_cli_versions.py`.

The benchmark responder image inventory is checked by
`scripts/check_responder_image.py` (newline-separated paths on stdin). This script
owns the recording-module allowlist shared by CI and
`tests/unit/sdo/operator/test_dockerfile_benchmark_scope.py`. The focused tests also
inspect all recording-module imports, including conditional imports, and reject
unapproved fastloop dependencies before building an image. Run them with
`uv run --extra test pytest -q -p no:tach tests/unit/sdo/operator/test_dockerfile_benchmark_scope.py`.

SDK/validator identities are computed from installed Go sources; no pin regeneration is needed after SDK changes, and `check_errors.sh` checks source binding and a real two-branch merge.

The proto gate runs `buf lint` and checks that committed Go/Python code matches
regenerated output. The v1alpha1 schema may evolve freely, including breaking
changes; compatibility with main is not a gate. Reserve deleted field numbers
and names so old stored state cannot be misread as new fields.

The no-LLM kind path is documented in
`benchmarks/sregym/experiments/fastloop/README.md`. It is intentionally run by
the experiment lead because it builds images and changes a kind cluster.
The scripted Level 1 check in `tests/unit/benchmarks/sregym/fastloop/`
exercises responder and reflection worker entry points with a copied hotel
seed, typed tasks, and a fake Kubernetes API, without a cluster.

The check before any kind or live run is the controller harness
(`go test -tags=integration -run '^TestHarness' ./runtime` from `sdo/operator/runtime`, about 30 s)
plus controller end-to-end scenario 1 (`bash scripts/test_controller_e2e.sh scenario1`, about 200 s).
Neither needs a cluster, container runtime or model.

### Source rendering on a real cluster (LK1)

The fast hermetic gate renders accepted source directly (no lock) and covers
rollout and restart, the FX1 memory publication regression, and continued
Service drift when another object cannot render.

The scripted Level 6 uses a private lane:

```bash
source /mnt/data/shli/cache/env.sh
flock /absolute/path/to/shared/rendered-lock.lock env \
  SDO_LK1_RUN_DIR=/absolute/path/outside/repository/lk1-e2e \
  bash scripts/test_rendered_lock_kind.sh
```

It creates that kind cluster if absent, loads `busybox:1.36`, and runs a real
Deployment with a mutable regular and init image. The production controller,
informer cache, SSA adoption and `sdo source` tool process two
scripted targetPort incidents with a human memory publication and controller
restart between them. Each incident requires a restored real HTTP route. It makes no model calls, deletes its test namespace, and
leaves the private lane available for inspection. It does not install a controller Pod or
exercise model-authored workers; those remain separate deployment gates.

### Tier 2a: toy real cluster

Use the toy commands in [the fastloop README](../benchmarks/sregym/experiments/fastloop/README.md#tier-2a-toy-real-cluster).
This tier needs Docker, kind, kubectl, Go and local runtime images, with no
external SREGym checkout or models. Calico is required: the unchanged production
broker refuses reflection when its default-deny validator canary can reach the API.

Harness rehearsal gate: `uv run python -m benchmarks.sregym.assurance.rehearse_harness --code-root <freeze> --lane <id>` runs three scripted toy scenarios (baseline, controller restart inside the injection pause, source repair with judge refresh) (about 15 minutes when the host is quiet; waits end on their owner's state, not a fixed cap) and writes a pass/fail summary JSON; see [scripted rehearsals](../.agents/skills/analyze-experiment/references/scripted-rehearsals.md#harness-rehearsal-gate).

The toy adapter retires a read-only sync Pod only after the real exporter has
finished and requested deletion; this avoids two 30-second sleep termination
waits per repeat. `--no-checkpoints` skips optional offline replay export, whose
investigator capture-reference issue is outside this tier. All real broker,
corpus, validator, outcome and strict-receipt checks remain enabled.

The benchmark unit suite can run without the optional external checkout. Tests
that inspect that checkout's actual worker source or registry explicitly skip
when those fixtures are absent; their assertions run when the checkout exists.

### Fastloop phase budget

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

Go-to-Python contract shapes are pinned by fixtures under `tests/fixtures/sdo/contracts/go/` (`sdo/operator/runtime/contract_golden_test.go`) and Python's strict models parse them (`tests/unit/sdo/contracts/test_go_contract_fixtures.py`). After an intentional Go encoding change, regenerate them with `go test . -run TestGoContractFixtures -update-go-contract-fixtures` in `sdo/operator/runtime` and rerun the Python tests. Python rejects `null` for a list field, so the fixtures must not contain one.

Agent turns are tested with agentshim's `FakeExecutor` through `tests/structured_turns.py`, which replays each provider's real stream format. Tests marked `live_agents` run the real Codex and Claude Code CLIs and are opt-in; cheap models keep them inexpensive:

```bash
SDO_RUN_LIVE_AGENTS=1 SDO_LIVE_CODEX_MODEL=gpt-6-luna SDO_LIVE_CLAUDE_MODEL=haiku \
  uv run pytest -m live_agents tests/
```

Live Codex or Kubernetes tests may require explicit markers, credentials, images, a cluster, and longer timeouts; do not infer production readiness from skipped external tests.

## Controller end-to-end

`bash scripts/test_controller_e2e.sh [scenario1|scenario2|all]` runs the real controller with no Docker, kind, network or model. It is the gate before any kind or live run, and it runs nightly (the `controller-e2e` job). It needs bubblewrap with user namespaces, `kubectl`, Go, `uv` and the envtest binaries (`KUBEBUILDER_ASSETS`, or `setup-envtest` on `PATH`). The code is `sdo/operator/runtime/e2e` (build tags `integration controller_e2e`, so ordinary Go CI never compiles it).

What runs as processes:

- `kube-apiserver` and `etcd` from envtest, started through the shared `runtime/envtestio` helper (`StartWith`: a static admin token, which the Pods mount as their service account token, and the service range and kubelet address type the fake kubelet needs): a real API server with no nodes and no controllers. The proxy and the kubelet present their own certificate, so `ca.crt` holds the API server's CA and that certificate.
- A fake kubelet (`e2e/kubelet_test.go`, `pods_test.go`, `runtime_test.go`, `network_test.go`). It is the one Node, the Deployment, ReplicaSet, Endpoints/EndpointSlice and Job controllers, the default ServiceAccount, and a container runtime that runs each Job or Pod container as a bubblewrap sandbox from the Pod spec (volumes, env, service-account token, termination message, init containers and sidecars). It serves pod logs and the usage endpoints the API server proxies to a kubelet. Application Pods are not executed: their phase follows the toy application (bad image: `ImagePullBackOff`; readiness path the app does not serve: not ready), and each gets a loopback address with a real listener, with Service forwarders and a hosts file standing in for kube-proxy and cluster DNS.
- The controller exactly as the toy lane installs it: the production `install.Installer` creates the controller Job, so `sdo-operator controller` builds the accepted detectors, starts its memory-host subprocess and runs against the API server (through a fault-injecting proxy). A killed controller is restarted by the fake Job controller.
- Agents: the registered no-model workers (`benchmarks/sregym/fastloop/agent_registrations`), launched as Jobs and standbys through the real worker, tool transport and broker. A wrapper command reads `/e2e/agent-mode` so a run can make the next agent crash or hang.
- The toy application and its faults: `apps/toy` manifests, injected and checked with the toy lane's own `apps/toy/faults.py`.

Every run checks the harness's two properties (`e2e/checks_test.go`): the run's evidence (controller logs, API state, accepted memory) is turned into a `sim.Log` and handed to `sim.Check`, so the runner and the in-process harness share one definition and one set of bounds (`GapFactor` 3, `Slack` 12 s):

1. Every incident ends. Every incident the controller opened has one terminal outcome once faults stop, a failed incident never keeps a later one from getting its outcome, and the controller never exits on its own (a Pod that ended and that the run did not kill). The runner also checks the one thing only the storage edge knows: one outcome document per incident at `.sdo/outcomes/<sha256(incident_id)>.json`.
2. Detection keeps running. Every detector in accepted memory is evaluated within three times its interval plus the slack while a controller process is up, through memory swaps, closure and agent work; a detector added by a memory commit appears with no controller restart. Downtime between two controller Pods and the run's declared fault windows are excused. A failing detector does not delay the others.

Scenarios call `Quiesce()` when they stop injecting faults, then `CheckProperties` (both) or `CheckEveryIncidentEnds` (API-error and hung-agent runs, where cadence says little).

Scenario 1 injects one toy fault, requires a scripted-agent repair, a reflection commit, a memory-host activation and no controller restart, then injects it again and requires the repeat to be cleared with no responder Job. Scenario 2 injects API latency, a crashing agent, `kill -9` of the controller mid-incident and a killed memory host. `TestControllerToleratesAPIErrors` answers 10% of API requests with 500 (and restarts the controller under them) and requires no controller exit; `TestControllerEndsHungAgent` hangs the responder and requires the controller to end it at its response deadline, record an outcome and handle the next incident. Fidelity limits are in [simulation-fidelity.md](simulation-fidelity.md#controller-end-to-end-fake-kubelet-and-limits). Run directories are kept on failure (`SDO_E2E_DIR` picks one; `SDO_E2E_KEEP=1` keeps it on success); each Pod's container output is under `pods/<namespace>_<pod>/logs/`.

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

Live-run findings about SDO behavior become seeded deterministic-simulation schedule events under `sdo/operator/runtime/sim_*_test.go`, with invariants checked at the affected boundary and after recovery. Cover the observed interleaving (including maintenance pauses, foreign objects, process restarts and slow workers), reproduce bugs before fixing them, and replay a failing schedule with `SDO_SIM_SEED` (plus `SDO_SIM_SCHEDULE` for schedule bytes). Keep each simulation partition below five minutes. Benchmark-harness bugs instead get benchmark-adapter tests.

### Benchmark boundary

Tests for `benchmarks/sregym/adapter/` should prove that benchmark resources are added through the runtime extension interface and that strict receipts derive from durable controller/broker evidence. Add architecture tests that reject imports from SREGym packages in production modules.

## Test style

- Write a failing reproducer before fixing a bug; see [Fix the class](#fix-the-class-not-the-instance).
- Test observable contracts and persisted evidence, not private fields or call order that is not part of the protocol.
- Prefer small explicit fakes over deep mock graphs.
- Use `tmp_path` and temporary Git repositories for repository mutations.
- Mock external subprocess boundaries in unit tests; do not require local `kubectl`, Docker, Go downloads, or model APIs unless the test is explicitly external.
- Assert failure paths: malformed structured responses, timeouts, stale commits, ownership violations, invalid detector code, interrupted rollouts, and duplicate acknowledgements.
- Keep one behavior per test and use names that state the invariant.

## Fix the class, not the instance

Fixing a bug means making its whole class impossible, not patching the one case you saw.

1. Before patching, ask whether the failing mechanism can be deleted. Deleting it is the cheapest fix for every instance at once.
2. Name the cause and search for earlier bugs with the same cause. On a second instance, fix the cause at its one owner (one rule, one budget source, one start path). Never add another exception or special case.
3. Guard the class with a ratchet, property test, or sweep that would fail for the next instance. Add a test that fails at the buggy commit as well.
4. Find bugs in batches: run the scripted (model-free) rehearsal over every planned problem before spending live model runs.

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

### Plugin boundary checks (archcheck)

`sdo/operator/internal/archcheck` enforces the core/plugin-family boundary from one rule
table, `rules.go`. It runs in `scripts/check_go_core.sh` (about 1 s) or alone with
`cd sdo/operator && go test ./internal/archcheck`. It parses imports and syntax without
type-checking, so negative-control fixtures under `testdata/` need no real dependencies.

- R1 core packages import no tool library (helm, kustomize, go-containerregistry, buildkit)
  and no plugin implementation subpackage (`render/<x>`, `imagesrc/<x>`, `upstream/<x>`,
  `telemetry/<x>`).
- R2 an implementation imports only its family interface and support package, `contracts`,
  `sdk`, stdlib and tool libraries: no core package, no sibling.
- R3 only `operatorcli` and tests import implementation subpackages.
- R4 no type assertion or type switch to a plugin config type (the `*Strategy` messages
  listed in `ConfigTypes`) outside its owning package. A new `*Strategy` type must be
  added to the table or the check fails.
- R5 ratchet: `ManifestPaths`/`manifest_paths` per-file counts may only fall.

Today's violations are `//archcheck:` directives in the violating file, so parallel PRs
never edit one shared table:

```go
//archcheck:allow R4 until=S3-13b reason="generic registry switches on the raw strategy"
//archcheck:ratchet R5 count=3 until=S3-11c
```

`until=` names the PR that removes the transitional form and must be in `TransitionalPRs`
in `rules.go` (a closed list; only the PRs that add or retire transitional forms edit it).
A violation without a directive fails; a rose count fails; a fell count fails with "lower
count= in this file"; a directive with nothing to match is stale and fails; an unknown
`until`, malformed directive, or duplicate (including one also in `rules.go`) fails. A PR
that edits a counted file adjusts the directive in the same file. Never add a directive to
make a new violation pass; fix the import instead. Generated `*.pb.go` files cannot carry
directives (generators overwrite comments), so their ratchet rows stay in `rules.go` and are
edited only by the serial proto PRs. Each rule and each directive failure has a fixture in
`testdata/` and `TestNegativeControls`/`TestDirectives` fail if one stops firing. See also
[`docs/architecture.md`](architecture.md).

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

The Python object-ref parser and Hypothesis tests for `sdo act` receipt recording use
`tests/fixtures/sdo/contracts/object_ref_grammar.json` (`test_recorded_actions.py`).

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

## Abort and quiescence

Start with the stop/abort contracts (Python; the Go stop tests were deleted, and the kind smoke below covers the real thing):

```bash
uv run pytest tests/unit/benchmarks/sregym/adapter/test_abort.py \
  tests/unit/benchmarks/sregym/adapter/test_processes.py \
  tests/unit/benchmarks/sregym/adapter/test_detached.py \
  tests/unit/benchmarks/sregym/adapter/test_native_persistent_ops.py
```

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
with typed `AGENT_TIMEOUT` evidence, and open work that lost its owner (a running
dispatch with no Job, or a finished responder with no closure past the
verification bound plus a grace) with `OWNER_LOST`, also while paused; both
require fresh observations for a new incident. The controller harness and the end-to-end scenarios exercise these paths.

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

### Runtime CI shards

The controller matrix keeps eight runtime runners and ten logical partitions:
`reflection`, `reflection-mongo`, `reflection-programs`, `contracts`, `rendered`,
`simulation`, `schedules`, `corpus`, `recovery`, and `remaining`. Run a partition
with `python3 scripts/test_runtime_shard.py --shard reflection` from the repository
root. Names are historical labels: measured expensive tests are assigned by cost,
so a reflection test can run in the rendered partition.

The runner discovers compiled tests across every module package, including
examples and fuzz seeds, and checks exhaustive alphabetical ownership before
balancing. `scripts/runtime_test_costs.json` stores measured top-level costs;
longest tests are assigned first to the least-loaded existing runner, with stable
name tie-breaking. Unmeasured tests retain their alphabetical owner and a small
planning estimate. The paired `reflection,corpus` and `simulation,remaining`
partitions are balanced as one runner each. Matrix changes must preserve that
mapping; the focused tests verify all eight allocations without adding a job.
Replay, chaos and explorer tests keep their dedicated jobs.

Race instrumentation, `-parallel=2`, every nested scenario and seed, and the
five-minute Go package timeout remain unchanged. A shared JSON watchdog bounds
runtime test execution and discovery/linking to 210 seconds (70% of the job), leaving room for
runner setup within the five-minute job deadline. It kills the entire process
group on a regression and reports the slowest and still-running test names.
For parents with parallel children, timing uses JSON lifecycle timestamps minus
any top-level pause, because Go's parent `Elapsed` excludes parallel-child waits.
The restart sweep, shared-memory modes, broker matrix and trust-property worlds
overlap independent scenario waits under
the existing two-test concurrency cap; all repositories and process state stay
owned by their scenario. The trust property precomputes the same seed-186 choices
and arrival permutations, then retains every prefix invariant. Two measured
placement corrections use that recovered capacity without reshuffling already
validated runners.

Update the cost profile from complete `go test -json` runs, then inspect every
runner's resulting duration. `tests/unit/sdo/operator/test_runtime_shards.py`
checks deterministic assignment, unchanged coverage, matrix ownership and invalid
cost rejection. New slow tests fail the named budget guard rather than silently
consuming the whole hosted timeout.

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
      proposed_artifact_ids: [example]
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
```

The generic parser tests cover
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
Replay uses these generators with captured command transport.

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

PRs and merge groups run `static checks`, two `Python tests` shards (`core`, and
`fastloop`, split by `SDO_PYTEST_SHARD` in `test_python.sh`), two `Go tests` shards
(`validate`, the compiler-backed memory validator, and `rest`; `test_go_ci.sh pr <shard>`),
and `Required PR CI`. Shards are independent jobs that each build from the Go
caches (no compiler artifact transfer); every job has a five-minute cap and
targets about three minutes. The aggregate requires every job to succeed (skips and
cancellation fail closed), combines the Python shards' coverage data and enforces the
`pyproject.toml` floor over their union. `check_errors.sh --python-suite` already
includes types, architecture and proto; the architecture tests run in the Python
`core` shard. Go modules use one temporary workspace per shard and one `go test`
invocation, with `-trimpath` allowing compiler entries to survive generated-workspace
path changes. Main saves each Go job's cache under its own key (`ci-shared-v1` for static
checks, `ci-tests-v2-...-<shard>` per Go shard) after deleting compiler entries the run
did not touch, so restores stay small; Actions caches are immutable, so sharing a save key
would let the first finisher discard another job's builds. Restores fall back across
dependency-hash changes; Go validates compiler entries by content. Real compiler tests use
the `integration` tag in the same invocation (PR and nightly).

#### CI budgets and ratchets

Every number lives in `.github/ci_budgets.json` and is enforced in code
(`tests/unit/sdo/operator/test_ci_guards.py`); lower a number there when a job gets faster, never raise one to make a job pass.

- Timeouts: `timeout_minutes` per workflow job is a ratchet; a job whose `timeout-minutes` exceeds its entry (or is missing from the table) fails the test.
- Main push runs are never cancelled (`ci.yml` gives each push its own concurrency group), so each merged commit is fully tested and refreshes the caches. `Required PR CI` stays the single aggregate required check over every shard.
- Slow tests: a PR-tier test over `slow_test_seconds` (10 s) fails its job (`scripts/check_slow_tests.py` reads `go test -json` and pytest `--durations`). Fix it, move it to the nightly/integration tier (`testing.Short` skip or the nightly ignore list), or as a last resort add it with a reason to `slow_tests`. Go keys are `<package> <TopLevelTest>`; Python keys are node ids without a parameter suffix.
- Cache size: main saves a Go cache only if `scripts/ci_cache_guard.sh` measures its compressed archive (what Actions uploads) at or under `cache_limit_mb` (1 GB); otherwise it skips the save and fails the job.
- `CI health` (hourly, `scripts/ci_health.py`) summarizes per-job p50/p95, merge-queue enqueue-to-merge latency and cancelled/rerun/ejected rates over the last `health.runs` runs created after `health.since`, and fails when any PR job p95 exceeds 180 s or queue p95 exceeds 8 min. After a fix, move `health.since` forward so the old runs leave the window.

`Nightly CI` runs daily and through `workflow_dispatch`; its Actions conclusion reports failures. It has one
Go job (every package under `-race`, without `-short`, with full simulation seeds, then the bounded fuzz campaigns),
the controller end-to-end scenarios, the slow Python suites that PR CI ignores, and the image builds.

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

Source the cached environment as described above before these commands:

`SDO_SIM_RUNS` changes sweep length. The 45-second host-time budget is checked
between seeds; individual seeds must also terminate (use `go test -timeout`).
Tests that need processes or real sockets must be integration-tagged. Use the
[fidelity ledger](simulation-fidelity.md) to distinguish modeled edges from real
cluster coverage; the memory-host and full controller driver remain follow-up work.


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

Real compiler, CLI, socket and kind tests carry `//go:build integration`, and PR and nightly
`scripts/test_go_ci.sh` both pass `-tags=integration`, so tagging never removes a test from CI. The
determinism allowlist is empty and cannot grow. `split_go_integration.sh` reports AST dependency
candidates for retagging and moves explicitly selected declarations, preserving embedded Go strings;
compile both build selections after a move.

Production code exposes these seams so tests can use virtual time and seeded randomness (a
`testing/synctest` bubble, one clock per run):

- `RuntimeOptions.Clock` drives controller wakeups, timestamps, observers, lease renewal and executor waits;
  `NewLeaseElectorWithClock` shares it with the Lease.
- `PlanExecutor.Now`, `.Sleep` and `.Jitter` serve program and responder receipts, diagnosis and trusted
  fast-path settling, and resource-version retries.
- `KubernetesCacheConfig.ResyncPeriod` lets a real client-go informer run in a bubble: its disabled-resync path
  selects on a package-global channel created outside the bubble, so tests set a positive virtual resync.
- `NewResponderLoopWithOptions` accepts a replayable session-ID source (a 32-character lowercase hex namespace).
- Maintenance, traffic, Service reachability, rollout and informer-observation polling accept client-go clock
  interfaces.

Checks/Python CI facts that still hold: compiler-cache restores fall back across dependency hashes within one
Go version (submodule initialization changes `hashFiles('**/go.sum')` between restore and save, so an
exact-hash-only fallback restored a stale cache). The Python runner builds each protocol executable once from
the current checkout into a temporary directory shared by its workers; executables are never restored. The uv
download cache retains Pyright's bundled engine (`prune-cache: false`); the runner supplies Node. Pinned proto
tools and the verified golangci-lint binary share a versioned tool cache. Formatting overlaps static checks, and
`check_errors.sh --python-suite` delegates the architecture tests, the fast Python ratchets and the Python
contract tests to the full Python suite, and the Go parity tests to the Go tests job (default local runs run
them all). Per-check timing lines and `--durations=30` remain in hosted logs. Identical code has run in 115 s or
187 s on different runners, so compare a job time with several runs, not one.

### Go-backed Python contracts

Tests marked `go_integration` execute real Go builds, generated controllers, or
restart processes. Unmarked Python tests may run the prepared operator, receipt checker and
submission guard but never compile them: `tests/conftest.py` builds those three
content-addressed executables once per session (CI and `scripts/test_python.sh` export them
first), and a PATH shim rejects any other compiler command outside marked tests. Add the
marker to future contracts that compile Go. Compiler parallelism is bounded for parent and
nested builds with `GOFLAGS=-p=2` and `GOMAXPROCS=2` on hosted runners.

Prebuilding a fixture executable does not warm Go commands it launches itself.
The broker health-bootstrap fixture therefore runs its complete validation on a
disposable accepted repository during fixture preparation. Preparation and the
timed fresh-repository invocation share the same compiler cache and `-trimpath`,
including generated controller tests, vet and the CGO-disabled traffic prober.
Its operation deadline stays at 60 seconds. To verify preparation from scratch:

```bash
GOCACHE=$(mktemp -d) uv run --extra test pytest -q -p no:tach \
  tests/unit/benchmarks/sregym/fastloop/test_eval_guards.py::test_fresh_guard_accepts_actual_broker_health_bootstrap \
  --durations=5
```

## Recorded Kubernetes client generation

`scripts/gen_kube_read_clients.sh` derives the read-only built-in interfaces from
client-go's pinned Go AST. Run it after updating the SDK's client-go dependency;
`--check` reports drift without writing files; `check_go_errors.sh` runs it.
### Concurrent shared memory

This is a hermetic plumbing proof, with no cluster or model. Cross-cluster shared
storage and live parallel-lane runner proof are separate benchmark work.

Fastloop checks saved controller and validator image source revisions against
the code under test before starting its SREGym worker, verifies lane image
content against local images, and runs the validator's Go toolchain as nonroot
in a read-only offline container. `scripts/build_sdo_images.sh` records the source
revision in the existing Bake graph. Missing or stale labels require rebuilding
through that script and delivering through `fastloop up`; tag names alone do
not prove provenance. An image's identity covers exactly the files its build
context sends: `image_provenance.py` derives the roots from the Bake target and
Dockerfile stage graph and drops what `.dockerignore` lists (Go `_test.go` files,
`testdata`, operator Markdown), so test-only, docs-only and eval-only commits keep
built images valid and any change to an image input invalidates them. Focused regressions live in
`tests/unit/benchmarks/sregym/fastloop/test_source_preflight.py` and `test_dev.py`.


GitHub Actions expression contexts are checked locally by
`scripts/check_actions.sh`, called by `scripts/check_errors.sh`. It runs pinned
actionlint1.7.7 with external shell/Python linters disabled; Ruff and the existing
shell gates retain those responsibilities. This catches invalid job-level
contexts before GitHub rejects the workflow without creating a check run.


The no-fault rehearsal also runs `sdo-observation-check` in the installed controller Pod before
accepting or reusing setup proof. It exercises production `DatabaseUsers` reads for discovered
MongoDB containers without models, mutation or Secret reads. Each workload returns a safe named
failure stage; namespace/selector discovery failures fail setup. Registry-rewritten images retain
Mongo identification through declared container names and listener ports. Source-declared auth
references can override a database target for a read-only reproduction with
`go -C sdo/operator/runtime run ./cmd/sdo-observation-check --namespace NS --kubeconfig PATH --target JSON`.
The target's `auth` references authenticate the catalog read separately from any create-user password.


## Context engineering gates

All registered agents use `sdo.agents.prompt_templates` for construction and provider dispatch. Every template placeholder has an explicit entry in `sdo/agents/prompt_sections.json`, referring to a cap in `prompt_budgets.toml`. Required literals and substitutions raise `PromptBudgetError` when their aggregate exceeds its cap; they never become summaries. Producers explicitly mark optional data for ranked trimming with omission counts and private session-file references. Reflection keeps every per-cause question and complete repair receipt in required parts, with settled evaluations and findings under the original optional caps. Repeated parts share an aggregate cap; complete data is never discarded. The renderer rejects unknown sections and placeholders. Nested rendered values preserve their own section caps; plain placeholders use their explicit mapped caps. Repointing an existing placeholder to a larger cap is also a raise: first raise its original budget in an isolated manifest-only `Budget-Reason:` commit, then remap downward.

Run the registry, real-scale producer, golden and provider-session checks together:

```bash
uv run --extra test pytest -q -p no:tach tests/unit/sdo/agents/prompts
uv run python scripts/update_prompt_goldens.py
uv run python scripts/check_prompt_budgets.py --base origin/vic-lsh/dev/sdo-improve --head HEAD
```

`test_context_registry.py` enumerates capability registrations and requires a production-rendered fixture for each implementation/capability. Its AST scan rejects direct provider calls, independent template evaluation and inline prompt construction outside the single owner; negative controls prove the guard fires. The template sweep enumerates every placeholder on generated ~185k-character values and requires an actionable overflow error. The always-run required-content ratchet checks all production renders and generated profiles at synthetic high-cardinality profiles, including fresh-session retries and shell guidance. Responder briefs retain every performed stage action; continuations retain every required row and exact action/finding identities, and derive their total limit from the manifest. No new template or role can silently inherit an unmeasured exception.

`test_production_caps.py` enumerates every CLI command and MCP registration, requires a matching real-scale fixture, and invokes the actual output boundary. CLI help/errors/stdout/stderr share a runtime cap; MCP dispatch caps every result and error. JSON callers receive a bounded envelope with the complete file path. Listing descriptions have their own caps; longer help lives in files. Full tool replies and prompt values are private, content-addressed session files under `$SDO_SESSION_SCRATCH/context/`, or `$SDO_SESSION_DIR/context/` for generic workers (standalone calls use `/tmp/sdo-context/`). Playbook inventories list logical IDs and one-line README `description:` metadata; open only relevant documents.

`check_errors.sh` runs the registry ratchet and `scripts/check_skill_sizes.py`. The size lint globs all Markdown below `sdo/skills/`, including future skills/references, and rejects SKILL.md over 100 lines/1,200 words or reference files over 2,000 words/12k characters (all file extensions). SDK references are generated indexes plus per-package topic files; `gen_skill_references.sh --check` checks the entire bundle.

Fastloop `preflight` runs the offline live-shape gate before host checks. It generates a 100-finding/100-playbook incident, renders responder/reflection prompts, emits each registered CLI command's text and JSON response, and reads the scripted provider's real session file. The hermetic shard runs the same producer/consumer check and negative controls. Actual scripted-agent turns also check their persisted provider sessions before returning a result. Budgets currently cap responder messages at 12k characters, reflection messages at 80k, and total tool text at 131,072 Unicode characters; the manifest is authoritative.

For a saved provider session:

```bash
uv run python scripts/check_session_context.py /absolute/path/to/rollout.jsonl --role reflection
```

The reader measures canonical Codex response items or Claude messages, including list-valued custom tool outputs and resumed messages, without double counting event mirrors. Missing or unsupported evidence fails closed. A gate failure points to the measured dimension and configured cap. This is a context-shape check; model repair quality still requires the separately authorized replay/live tiers. Keep recorded inputs read-only and measurement outputs outside the repository.


## Build tags

`//go:build integration` marks a test file that itself references real time, environment,
processes or sockets (`docs/go-purity.md`). It does not decide whether a test runs in PR CI;
`-short` and the nightly job do. A file that only calls a tagged helper loses its tag when the helper
moves behind a declared edge (`internal/testutil/gitio`, `internal/testutil/toolio`) or takes an
injected instant. Run `go vet ./...` with and without `-tags=integration` after retagging.

## Heavy tests must earn their cost

PR CI runs `go test -short`; nightly runs the same packages with `-race`, without `-short`, and with full
simulation seeds (`SDO_SIM_FULL=1`). A test that takes 10 s or more skips itself under `testing.Short()`
and says that nightly runs it. Today: `TestAssuranceSeedsPassHealthJudgeCheck`,
`TestWorkedExamplesPassTheReflectionMemoryCheck`, `TestMemoryNewDetectorScaffoldsBuildVetAndPassTheirTests`.
There are no name lists.

A test over about 2 s, or one that builds, spawns or connects for real, stays only if it catches a
deliberate production fault that no cheaper test catches: apply the fault, the fast suite passes,
the heavy test fails. Record the fault with the test. Failing after a refactor is cost, not value.
