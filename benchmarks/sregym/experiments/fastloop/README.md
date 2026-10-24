The smoke stream names the two original mixed-fault problems (i06 and i10). The explicit stream injects their component faults with `inject_composite`, using the single-fault IDs from the playbook trial's repeat config.

TODO: The SREGym fork checked out in this worktree still needs the `mf__hotel_reservation__n2__s1773744812__v_` and `mf__hotel_reservation__n2__s1085951852__v_` composite specs and the hotel `target_port` variants for `reservation` and `geo`. Until those are registered, run `pbprog_explicit.json` after the target-port variants land.

## Streams

| Stream | Incidents | Purpose |
| --- | ---: | --- |
| `pbprog_smoke.json` | 2 | Original registered two-fault composites |
| `pbprog_explicit.json` | 2 | Explicit single-fault IDs for those composites |
| `pbprog_repeat.json` | 8 | TargetPort/image reuse, including mixed incidents |
| `pbprog_repeat_detectable.json` | 12 | Health-visible repetitions |
| `pbprog_compose.json` | 10 | Composition and hidden-fault coverage |
| `pbprog_drift_standalone.json` | 2 | Standalone targetPort drift response |
| [pbprog_latent.json](pbprog_latent.json) | 10 | Eight distinct targetPort targets with two interleaved health controls; first catch versus learned invariant sweep |

The [latent-fault catalog](latent_faults.md) records code and run evidence,
available targets, oracle limits and feed gaps. The latent stream uses one
confirmed repeatable class: newly injected NetworkPolicies are already watched
at this commit but lack accepted source ownership for first responder assessment.
Use the trial's structural-health objective, `--detection-timeout 180` and
`--continue-on-error`. The assurance seed also enables functional traffic
detectors, so its detections must be reported as health catches. Record the
accepted seed and opening detector; a program detection path does not imply
responder-free mitigation. No live result is claimed for this stream.

# Mounted-source development loop

For a hotel latent trial reusing the historical structural-health seed, first
align its Jaeger source manifest with the captured healthy tracing alias:

```bash
uv run python -m benchmarks.sregym.fastloop.hotel_latent --run-dir "$SDO_FASTLOOP_RUN"
```

This commits the source-only correction in that run's application checkout and
updates its reset seed; it leaves the health objective and detectors unchanged.
Add `--sweep-service recommendation --sweep-service geo` to an SDO targetPort
stream to model those repeats as imported Services. Ownership is cleared after
controller adoption, immediately before injection; clearing it before startup
is insufficient because adoption restores it. Check the accepted outcomes for
`source="periodic program sweep"` and `binding_source=hint`. This is an explicit
benchmark setup, not a change to production drift admission.

`up` mounts the current worktree by default. Bare `--dev-mount` makes that
explicit; `--dev-mount PATH` selects another worktree. SREGym lanes can opt out
with `--no-dev-mount`, which warns that source edits require rebuilt images.
Toy lanes require mounts. Run the cheap host gate before building new images:

```bash
uv run python -m benchmarks.sregym.fastloop preflight
# Only for dependency, tool or Dockerfile changes; use a new tag:
SDO_IMAGE_TAG=my-frozen-tag bash scripts/build_sdo_images.sh
```

`up` repeats host and requested-image checks before its first compiler or Docker
build. Missing tools, an unavailable Docker daemon, an invalid worktree or a
missing image fail immediately. Live launch assurance (quota, pins, parity and
host capacity) still applies before spending model tokens.

Each lane owns `<cluster>-registry` (`registry:3`), labelled `sdo.fastloop.lane`,
with a dynamically allocated loopback port and storage retained across resets.
It joins the kind network; containerd hosts.toml routes pulls to that container.
Images keep their original names inside the nodes. Pushes retain layers and only
missing layers transfer. Existing unlabelled registries are never adopted.
`fastloop down --run-dir <lane> --delete-cluster` removes the cluster and then
its ownership-checked registry container. Plain `down` and resets retain it.
Unchanged warm lanes skip delivery; resets do no image work. Registry setup,
push or pull failures trigger automatic archive fallback and a clear stderr
WARNING containing the error, image list and elapsed fallback seconds.
`fastloop.json` and every incident row record `image_delivery` and
`image_delivery_reason`; `image_delivery.jsonl` keeps delivery history. Each
incident row rereads that history after the incident, including source-deploy
fallbacks triggered during the run. Run warns
again while the lane remains in `kind-load-fallback` mode. A successful registry
refresh of all affected images clears it. There is no manual flag to prefer archive loading.

Only the controller image stores Go and the offline dependency/build caches.
The installer passes that image as the required `--toolchain-image` setting,
independently of `--prober-image`. Direct controller launches must set it too;
missing configuration fails before building, migration or Kubernetes access.
Worker and validator init containers use that image to populate a private
emptyDir, mounted read-only into compiler-free runtime containers. The init
container has no candidate or provider-credential mounts. Compilation, vet,
replay and network isolation remain required. Scripted images omit model CLIs
from their ancestry. Bake shares OS and dependency stages; never flatten images.

Use an existing set of SDO controller, responder, and validator images for the
first `up`. The worktree is mounted into every kind node at `/opt/sdo-dev`; the
Pods use its Python packages and a host-built `sdo-operator` at
`.fastloop/sdo-operator`. The host build uses the warm Go cache. `reset` reuses
the cluster and images, restores the captured application and seed repository,
clears controller state and sessions, and checks health and the deployment
problem's mitigation oracle.

```bash
export SDO_FASTLOOP_RUN=/absolute/path/to/fastloop-run
export SDO_FASTLOOP_SEED=/absolute/path/to/accepted-lifecycle-seed
uv run python -m benchmarks.sregym.fastloop up \
  --run-dir "$SDO_FASTLOOP_RUN" --seed "$SDO_FASTLOOP_SEED" --dev-mount "$PWD"
# After an SDO source edit; no image build or new cluster:
uv run python -m benchmarks.sregym.fastloop reset \
  --run-dir "$SDO_FASTLOOP_RUN" --dev-mount "$PWD"
uv run python -m benchmarks.sregym.fastloop run \
  --run-dir "$SDO_FASTLOOP_RUN" --agent sdo \
  --stream tests/fixtures/sregym/fastloop/pbprog_repeat.json \
  --diagnosis-grading off --time-scale 1 --continue-on-error
```

`--time-scale` defaults to 1. Live LLM runs, including smokes, use 1.
Values below 1 are refused before runtime setup unless SDO uses the repository's
`benchmarks/sregym/adapter/scripted_registrations` (explicitly selected, or the
toy lane default). Custom registrations are conservatively treated as live.
This keeps live verification and responder deadlines from being shortened.
For scripted-agent runs it scales controller detector intervals, finding
debounce, drift baseline settle, diagnosis settle, verification waits, the
fastloop reflection wait, controller and fault-gate polling, recovery health
polling, and composite stability and deadline waits. `--continue-on-error` records a failed incident and
moves on only when recovery has passed its oracle. A new dev lane uses the
`fastloop-dev-w` cluster prefix; choose another prefix if that name is taken.

# Recovery isolation

For SDO streams with latent faults, add `--detection-timeout SECONDS` to
`fastloop run`. If no incident opens within that window, the incident gets a
failed row with `no incident opened within Ns`, the oracle is evaluated, and
baseline recovery runs before the next incident. Once an incident opens, the
normal verification deadline still applies. Without the flag, fastloop keeps
waiting for the full verification deadline (`--timeout` plus 300 seconds for
single faults, or `--composite-deadline` for composites).

`up` adds full application workload specs for its namespace or namespaces to
`cluster_baseline_state.json`. After each fault recovery, fastloop deletes
agent-created workloads, restores changed baseline specs, waits for health,
then checks the recovery oracle. SDO's control namespace and its repository
are outside this reset. Older run directories without an application snapshot
use fault-only recovery and mark `cleanup_skipped_reason` in each incident row.
`cleanup_deleted_counts` and `cleanup_restored` show what was reset.

# Scripted SDO loop (kind, no LLM)

Level 1 exercises both scripted worker entry points with a fake Kubernetes API:

```bash
uv run pytest -q -p no:tach tests/unit/benchmarks/sregym/fastloop/test_scripted_worker.py tests/unit/benchmarks/sregym/fastloop/test_scripted_image_build.py
```

The test copies the accepted hotel seed when it is available at the experiment
path, or its checked-in manifest and Helm template fixture elsewhere. The
scripted source repair requests rendered accepted manifests from the Go controller.

Run from this repository root with an accepted lifecycle seed. The seed must
contain `.sdo/diagnostics/manifest.yaml` and `.sdo/lifecycle-provenance.yaml`;
the scripted registrations replace the responder and reflector. The
registration override rejects an unseeded workspace before cluster access.
Choose a new run directory for each build:

```bash
export SDO_SCRIPTED_SEED=/absolute/path/to/accepted-lifecycle-seed
export SDO_SCRIPTED_RUN=/absolute/path/to/new-fastloop-run
SDO_IMAGE_TAG=scripted-fastloop SDO_SCRIPTED_WORKER=1 bash scripts/build_sdo_images.sh
uv run python -m benchmarks.sregym.fastloop up \
  --run-dir "$SDO_SCRIPTED_RUN" --seed "$SDO_SCRIPTED_SEED" \
  --controller-image sdo-controller:scripted-fastloop \
  --responder-image sdo-sregym-responder:scripted-fastloop \
  --validator-image sdo-detector-validator:scripted-fastloop
uv run python -m benchmarks.sregym.fastloop run \
  --run-dir "$SDO_SCRIPTED_RUN" --agent sdo \
  --agent-registrations benchmarks/sregym/adapter/scripted_registrations \
  --stream tests/fixtures/sregym/fastloop/pbprog_repeat.json \
  --diagnosis-grading off --reflection-session fresh --timeout 300
```

The worker image keeps the production SDO tool, Go CLI, and typed worker
transport. Its registered scripts call `sdo diagnose`, `plan`, `apply`, and
`verify`; reflection calls `sdo-operator program scaffold` and `program
propose --corpus-from-incident`. The image removes model CLIs. This stream
uses the existing target-port and bad-image faults; it requires those fault
variants in the local SREGym checkout. Do not use `pbprog_smoke.json` for this
mode until its two composite specs are present.

Unexplained drift opens an ordinary responder incident. The responder must establish a source contradiction and an observed violated relation or impact before repair; quiet health and missing port declarations alone are insufficient. An unsure result remains unresolved. Only an exact checked benign label, stored in accepted outcomes, suppresses a repeated observation. Fail-soft reset preserves accepted outcomes; a seed rollback discards their labels. Reset first cancels and awaits all Jobs, then cleans captures and checkouts with a temporary Pod.

`fastloop run --drift-tokens-per-hour N` bounds repeat drift-only responder reasoning (default 1,000,000). New drift keys bypass spent retry budgets; health dispatch and attachment to an open incident do not wait. New `run_metadata.json` files declare accounting version `responder-drift/v2`: merged reasoning appears once in `responder_tokens`; `drift_tokens` remains a legacy zero field. Old result rows retain their recorded investigator costs.

## Offline stage replay

Each SDO `fastloop run` writes `checkpoints/<incident-number>/<stage>/` under
`results/<run-id>/`. A checkpoint keeps the exact worker task and result
ProtoJSON, its logical memory snapshot or accepted commit in the task, the
matching broker ledger when settled, and redacted namespace snapshots and
recordings from the incident corpus. Capture files are stored once by SHA-256
under `checkpoints/blobs/`. Tasks and results have a 1 MiB checkpoint limit,
so an oversized worker result remains available to debug its transport failure.
Corpus snapshots have the SDK's 4 MiB per-file limit and a
32 MiB bundle limit. Keep the run directory private because exact agent
requests and results can contain incident text.

Replay from the repository root, with the results run directory rather than
the warm environment directory:

```bash
uv run python -m benchmarks.sregym.fastloop replay \
  --run-dir /path/to/fastloop/results/<run-id> --incident 0 --stage responder
uv run python -m benchmarks.sregym.fastloop replay \
  --run-dir /path/to/fastloop/results/<run-id> --incident 0 --stage reflection
uv run python -m benchmarks.sregym.fastloop replay \
  --run-dir /path/to/fastloop/results/<run-id> --incident 0 --stage broker
```

`reflection` feeds recorded Codex outputs through the current reflection agent and restores the recorded direct file edits,
including corrections recorded as separate Go dispatches, then runs the shared complete
memory validator, captured-corpus provenance checks and real broker publication.
Each dispatch reports its logical artifact IDs, acceptance and temporary commit,
or the exact rejection. An unrecorded correction stops replay as `incomplete`;
no output is invented. All Git writes happen in temporary clones. No cluster,
Docker or live model is needed. The source repository must retain the accepted
outcome commit named by the checkpoint.

`responder` defaults to the scripted worker against captured open and fixed
snapshots. Select `--agent live` to use its recorded Codex turns. Select
`--agent scripted` explicitly for a scripted reflection. `broker` validates an
existing reflection result without re-executing the agent. Worker replay also
reports whether the current typed result matches the recorded result.

The default Codex registrations persist exact task/result ProtoJSON under each
exported session's `checkpoints/<request-digest>/`, independently of ConfigMaps. Completed reflection dispatches also record `file_edits.json`: version 1 contains paths, exact base64 file bytes (null for deletion), and executable modes. Bundles are bounded to 48 MiB encoded and stored by digest in the stage checkpoint. Replay applies those edits to an isolated checkout, then invokes the real Go broker and shared complete memory check. Proposal-only recordings fail with an explicit error; no proposal shim remains.
This includes failed reflection results and ordinary drift responder sessions. The run-level
stage checkpoint selects the latest dispatch; the exported session checkpoints
retain all dispatches. Model I/O lives in `model_io/<request-digest>/`, keyed by
prompt digest and occurrence so identical correction prompts cannot overwrite
earlier outputs. New recordings include `turn_index`: reflection replays those
outputs in order even after prompt edits, reporting changed prompt hashes in
`prompt_changes`. Older recordings require matching prompt hashes. Missing turns
fail without a model call. `--live-on-cache-miss` explicitly permits a live
fallback; omit it for Tier 3. The recorder saves no command environment or auth
files, and the runtime export retains its credential and cache exclusions.

Explicit `--agent-registrations`, including the scripted registrations above,
keep their own worker commands.

# Tier 2a: toy real cluster

The reusable [toy application](../../../../apps/toy/README.md) has three tiny HTTP
services, real Deployments, local readiness probes and a frontend-to-backend
journey. It does not import or populate `third_party/sregym`. Pinned Calico
provides NetworkPolicy enforcement, required by the production validator canary.
See the [decision and alternatives](../../../../docs/toy-cluster-design.md).

Prepare matching runtime images once (or use already built image tags):

```bash
export GOMAXPROCS=2 GOFLAGS='-p=2'
SDO_IMAGE_TAG=fastloop SDO_SCRIPTED_WORKER=1 bash scripts/build_sdo_images.sh
```

Up uses the standard kind node image and delivers runtime images through its
lane registry. Reuse that cluster and reset it for source edits; no custom
preloaded node image or archive preparation is needed.

```bash
export SDO_TIME_SCALE=0.1
export TOY_RUN="$PWD/.fastloop/toy"
uv run python -m benchmarks.sregym.fastloop up --app toy \
  --run-dir "$TOY_RUN" --cluster-prefix my-toy- --dev-mount "$PWD"
uv run python -m benchmarks.sregym.fastloop run --run-dir "$TOY_RUN" --agent sdo \
  --stream tests/fixtures/sregym/fastloop/toy_latent_repeat.json \
  --reflection-session fresh --diagnosis-grading off --time-scale 0.1 \
  --timeout 300 --detection-timeout 75 --no-checkpoints
uv run python -m benchmarks.sregym.fastloop reset --run-dir "$TOY_RUN"
```

The cluster name uses `--cluster-prefix` plus `--worker-id`; creation refuses to adopt an existing
cluster without its environment record. The source mount is mandatory internally
and defaults to this worktree. `run` and `reset` rebuild the host operator, so code
changes need no runtime image rebuild. Scripted responder, reflector and drift
investigator registrations are the toy default. No model calls are needed.

After that stream has accepted the program, run `toy_sweep.json` with
`--timeout 900` and the same other run flags. It models a healthy imported Service
by clearing field ownership before introducing targetPort 9999. The production
feed and sweep schedule are unchanged; the outcome must say
`source="periodic program sweep"` and retain `binding_source=hint`.

The stream injects targetPort 9999 on frontend, backend and catalog, then a
nonexistent catalog image. `apps/toy/faults.py` also supports
`toy_readiness_backend` (and the other service names): it changes `/ready` to a
404 endpoint. All app images use `imagePullPolicy: Never`. Each mitigation oracle
checks the actual repaired field; workload oracles also require the observed
Deployment generation and one updated, ready replica. Their `health-check` result
kind is shared with fastloop, but they are fault-specific cluster-state checks.

`reset` restores the app baseline and seed checkout, discards learned programs,
outcomes and toy drift verdicts, and restarts from the production-installed Job
spec with fresh server identity. It removes the stopped controller's leadership
lease, retains the PVC build cache, and waits for a fresh, adopted clear baseline.
The hotel tier retains its existing reset behavior, including cached drift verdicts.

`--no-checkpoints` disables only optional offline replay checkpoint export. The
real Go controller, broker, memory edge, captured-corpus validation, reflection,
strict receipts and outcome documents remain enabled. Replay export currently
fails for an investigator capture reference not found in the exported incident
corpus; that recording issue is separate from this tier.

The seed is hand-authored test input, with human goal metadata, source-bound
architecture, indexed playbooks and health-owned Go diagnostics in schema v3.
A Go test checks the complete seed through shared `memory/validate`, including
detector compilation. Scripted reflection proposes and
commits the supplied invariant program; this proves the learning machinery,
not model learning. Measured setup and incident costs are recorded in the testing guide.

The toy evidence exporter performs the real native export, then removes only its
already-deleting read-only sync Pod so the next export avoids a 30-second sleep
grace. No validation or receipt work is omitted. The full warm repeat measured
16.2 seconds; see the testing guide for cold setup costs and remaining targets.


The schema-v3 integration recheck measured cold up **126.38 s**, reset **8.95 s**,
and first invariant acceptance **132.12 s**. Backend/catalog repeats took
**19.02/16.99 s** (mitigation **2.72/2.59 s**); the image control took **19.52 s**.
The separate unowned-Service periodic sweep took **32.24 s**, with
`source="periodic program sweep"`, a learned detector and `binding_source=hint`.
All five oracles and strict receipts passed. Cold up used preloaded `unified4`
images; preparing/loading the current worker image and its 71.53 s controller
replacement were separate. The scripted worker image must
include main's generated-contract dependencies, including `protovalidate`;
source mounts do not add missing Python packages. Raw evidence is outside Git
at `results/toy-cluster-evidence/`.
