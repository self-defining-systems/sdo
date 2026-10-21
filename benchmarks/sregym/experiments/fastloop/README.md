The smoke stream names the two original mixed-fault problems (i06 and i10). The explicit stream injects their component faults with `inject_composite`, using the single-fault IDs from the playbook trial's repeat config.

TODO: The SREGym fork checked out in this worktree still needs the `mf__hotel_reservation__n2__s1773744812__v_` and `mf__hotel_reservation__n2__s1085951852__v_` composite specs and the hotel `target_port` variants for `reservation` and `geo`. Until those are registered, run `pbprog_explicit.json` after the target-port variants land.

# Mounted-source development loop

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
  --stream benchmarks/sregym/experiments/fastloop/pbprog_repeat.json \
  --diagnosis-grading off --time-scale 0.1 --continue-on-error
```

`--time-scale` defaults to 1. It scales controller detector intervals, finding
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
the scripted registrations replace the responder, reflector, and drift investigator. The
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
  --agent-registrations benchmarks/sregym/experiments/fastloop/scripted_registrations \
  --stream benchmarks/sregym/experiments/fastloop/pbprog_repeat.json \
  --diagnosis-grading off --reflection-session fresh --timeout 300
```

The worker image keeps the production SDO tool, Go CLI, and typed worker
transport. Its registered scripts call `sdo diagnose`, `plan`, `apply`, and
`verify`; reflection calls `sdo-operator program scaffold` and `program
propose --corpus-from-incident`. The image removes model CLIs. This stream
uses the existing target-port and bad-image faults; it requires those fault
variants in the local SREGym checkout. Do not use `pbprog_smoke.json` for this
mode until its two composite specs are present.

The drift stream can record an expected pre-existing difference for the
`jaeger` Service: SREGym replaces the source Service with a `kubectl-create`
object that has no ports. Its unchanged `unsure` verdict must remain recorded
without being investigated again after each repair. It does not block later
target-port drift incidents in the stream.

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

`responder` and `reflection` run the current scripted worker entry points in
an isolated source clone. The responder's Kubernetes reads and patches use
the captured open and fixed snapshots. `broker` runs the current Go memory
materializer, broker validation, gofmt and program build in a temporary local
repository. Replay never needs the warm cluster. It reports the new typed
result and whether it matches the recorded result, ignoring response
timestamps.

The default Codex worker registrations record each agentshim Codex CLI turn
under the exported incident session's `model_io/` directory, keyed by task and
prompt digests. Use `--agent live` to run the current agent code with those
recorded turns. A missing turn fails with both digests. Add
`--live-on-cache-miss` to call `gpt-6-luna` and record a new turn. Live replay
points `KUBECONFIG` at a local HTTP API backed by the captured snapshot.
Explicit `--agent-registrations`, including the scripted registrations above,
keep their own worker commands.
