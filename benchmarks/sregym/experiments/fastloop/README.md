The smoke stream names the two original mixed-fault problems (i06 and i10). The explicit stream injects their component faults with `inject_composite`, using the single-fault IDs from the playbook trial's repeat config.

TODO: The SREGym fork checked out in this worktree still needs the `mf__hotel_reservation__n2__s1773744812__v_` and `mf__hotel_reservation__n2__s1085951852__v_` composite specs and the hotel `target_port` variants for `reservation` and `geo`. Until those are registered, run `pbprog_explicit.json` after the target-port variants land.

# Recovery isolation

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
the scripted registrations replace only the responder and reflector. The
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
