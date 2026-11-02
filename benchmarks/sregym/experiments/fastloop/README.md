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
for the compiler toolchain. Direct controller launches must set it too;
missing configuration fails before building, migration or Kubernetes access.
Worker init containers use that image to populate a private
emptyDir, mounted read-only into compiler-free runtime containers. The init
container has no candidate or provider-credential mounts. Compilation, vet,
replay and network isolation remain required. Scripted images omit model CLIs
from their ancestry. Bake shares OS and dependency stages; never flatten images.

Use an existing set of SDO controller and responder images for the
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
  --diagnosis-grading off --continue-on-error
```

<<<<<<< HEAD
`--time-scale` defaults to 1. Live LLM runs, including smokes, use 1.
Values below 1 are refused before runtime setup unless SDO uses the repository's
`benchmarks/sregym/fastloop/agent_registrations` (explicitly selected, or the
toy lane default). Custom registrations are conservatively treated as live.
End-to-end harness verification and reflection budgets always use wall time: cold worker
startup, compilation and API reads do not scale with detector timers.
For scripted-agent runs it scales controller detector intervals, finding
debounce, drift baseline settle, diagnosis settle, verification waits, the
controller and fault-gate polling, recovery health
polling, and composite stability and deadline waits. `--continue-on-error` records a failed incident and
=======
There is no time scaling; every run uses real time. `--continue-on-error` records a failed incident and
>>>>>>> origin/vic-lsh/dev/sdo-improve
moves on only when recovery has passed its oracle. A new dev lane uses the
`fastloop-dev-w` cluster prefix; choose another prefix if that name is taken.

# Reused lanes: the row-start wipe

Every `run --agent sdo` row starts with two tiers (`fastloop/wipe.py`; the receipt `wipe-receipt.json`
in the results directory records `tier1_seconds`, `tier2_ran`, `tier2_seconds`, `tier2_reason`):

- **Tier 1, always.** Delete `<app>-sdo` and every namespace and cluster-scoped object that is not the
  lane's own (namespaces outside the infrastructure and app set; cluster objects new since the
  `<kubeconfig>.lane-inventory.json` that `up` records). Then run `--credentials-hook EXECUTABLE <app>-sdo`
  (the lane keeper creates the namespace and its credentials Secret; fastloop never reads or copies a value),
  reset the accepted repository to `--planned-commit` (the seed for a first try, the previous row's
  `accepted_after` for a repeat), and forget the controller registry. The normal native install follows.
  Nothing SDO wrote survives except the memory commit. A lane with no inventory adopts its current
  cluster-scoped objects on the first wipe. Target: under about 90 s.
- **Tier 2, only when needed.** The warm app stays when the previous row recovered cleanly, the restore
  invariant (`workload_identity` and `assert_clean_baseline`) holds and every app pod is serving (checked for
  up to one minute). Otherwise the app namespaces are deleted whole, PVCs and the data in them included
  (fault state such as a MongoDB user lives in the data, not in a Kubernetes object), the app is deployed
  again with `deploy --redeploy` from the image cache and the application baseline is captured again. The
  trigger is `tier2_trigger` in `wipe-receipt.json`: `current_row_data_faults` when the incoming
  problem contains any catalog `DATA_LEVEL_CLASSES` fault, `lane_last_injection_data_faults` when the
  lane's last injection did, or `lane_injection_unknown` when its durable state is absent, malformed
  or pending after an interrupted injection. The driver writes that state beside the lane kubeconfig
  (`<kubeconfig>.<namespace>.last-injection.json`) before the worker can mutate the app, and confirms it
  after injection. Run directories, seeded first-try copies and journal predecessors never supply lane
  history. `current_row_data_faults` and `previous_row_data_faults` in the receipt identify the triggering
  faults. Other triggers remain `unclean_attempt`, `previous_row_unrecovered`, `restore_invariant` and
  `pods_unhealthy`. A known non-data injection followed by a healthy non-data row keeps the warm app.

A namespace either tier deletes can stay Terminating on an injected or harness object whose finalizer
nobody will run (the FinalizerDeadlock fault's cleanup token). When the namespace reports remaining
finalizers or content, the wipe clears the finalizers of the objects still in it, inside the same
deadline, and records each as `namespace/kind/name` in `force_finalized` of `wipe-receipt.json`. It never
touches a namespace it did not ask to delete; if the namespace still does not finish, the row fails closed
naming the objects.

The lane-resource entry check and the restore invariant stay: they are the measurement-validity checks.

`up --redeploy` is repeatable for every app. The worker bridge routes SREGym's `Helm.install`, the
one entry point for the app and telemetry charts, through `helm upgrade --install`
(`fastloop/idempotent_helm.py`), so a lane that still holds the release no longer fails with
"cannot re-use a name that is still in use".

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

Namespace recovery captures ServiceAccounts, Roles and RoleBindings alongside
workload, ConfigMap, Service and PVC specs. It follows pod-template references
(including init containers, projected volumes and environment references),
StatefulSet services and claims, and RBAC references. Missing legacy capture
prerequisites may come from contained, hash-verified accepted rendered manifests;
captured installed specs retain precedence. Prerequisites are applied before
workloads. Before any reset writes, unavailable prerequisites are rejected by
namespace/kind/name. Secret contents and unowned cluster-scoped resources are
never restored; only name projections establish whether external dependencies
already exist. Recreating a namespace that requires an unavailable Secret needs
its owning deployment workflow before fastloop reset can proceed.

Seed restoration requires the harness's `seed-compatible/v1` recovery contract,
including when `--require-full-cleanup` is omitted. One `SregymFaultDriver.recover()`
call resets external fault state through the harness, restores every captured
application namespace, requests post-seed client refresh, then waits for health
and rollouts. This restores credential faults to the seed's backend state before
reapplying manifests and refreshes database connections afterward. It is safe to
retry the whole operation after a manifest or refresh failure. Failed or timed-out
cleanup stops slot reuse; successful reset alone does not verify full cleanup.
Older harness pins must be updated before seed-based cleanup can run. The benchmark
adapter never reads Secret contents or implements database credential resets.

Clean-baseline admission also rejects any remaining SREGym object labelled
`platform.io/injected=true`, including cluster-scoped RBAC. The worker removes
those objects after full harness recovery and before deploy/reset baselines,
then lists again to prove deletion. Partial composite recovery leaves other
injections intact. Secret cleanup projects only namespace/name metadata.
Unlabelled source objects and the SDO control namespace keep their existing
restoration contracts.

Hotel and social-network Wrk2 traffic runs in the harness namespace `sregym`.
The existing payload, request rates and oracle thresholds are preserved; the
manager and its Wrk2 client both target that namespace for creation and logs.
Traffic addresses the application frontend by cluster Service DNS. Warm-lane
restore starts the generator there and removes the legacy `wrk2-job`, its Pods
and `wrk2-payload-script` from the application namespace. Admission rejects
legacy traffic residue even when it has no injection label. The pinned harness
implementation is reused through the worker bridge; no production exemption
or SREGym pin change is required.

# Scripted SDO loop (kind, no LLM)

Level 1 exercises both generated agent scripts with a fake Kubernetes API:

```bash
uv run pytest -q -p no:tach tests/unit/benchmarks/sregym/fastloop/test_generated_worker.py tests/unit/benchmarks/sregym/fastloop/test_scripted_image_build.py
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
  --responder-image sdo-sregym-responder:scripted-fastloop
uv run python -m benchmarks.sregym.fastloop run \
  --run-dir "$SDO_SCRIPTED_RUN" --agent sdo \
  --agent-registrations benchmarks/sregym/fastloop/agent_registrations \
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

Unexplained drift opens an ordinary responder incident. The responder must establish a source contradiction and an observed violated relation or impact before repair; quiet health and missing port declarations alone are insufficient. An unsure result remains unresolved. Only an exact checked benign label, stored in accepted outcomes, suppresses a repeated observation. Fail-soft reset preserves accepted outcomes; a seed rollback discards their labels. Reset first cancels and awaits all Jobs and any orphan controller Pod, then retires only the transient controller-state and drift-verdict cache ConfigMaps so the next problem cannot resume a stale pending incident. Historical incident evidence ConfigMaps remain intact. A repository-preserving reset retains accepted Git and native broker ledgers while removing proposal and session scratch. An explicit seed rollback may remove old captures and worktrees with a temporary Pod; archive run history first.

New `run_metadata.json` files declare accounting version `responder-drift/v2`: merged reasoning appears once in `responder_tokens`; `drift_tokens` remains a legacy zero field. Old result rows retain their recorded investigator costs.

## Offline stage replay

Each SDO `fastloop run` writes `checkpoints/<incident-number>/<stage>/` under
`results/<run-id>/`. A checkpoint keeps the exact worker task and result
ProtoJSON, its logical memory snapshot or accepted commit in the task, the
matching broker ledger when settled, and redacted namespace snapshots and
recordings from the incident corpus. Capture files are stored once by SHA-256
under `checkpoints/blobs/`. Tasks have a 512 KiB limit and results a 1 MiB checkpoint limit,
so an oversized worker result remains available to debug its transport failure.
Corpus evidence has a 4 MiB per-file limit and a 32 MiB bundle limit.
Oversized parts are omitted with an `unrecorded_captures` name-to-reason map
in the stage manifest; exact worker results, the broker ledger and other captures
remain recorded. Replay reports these stages as `not replayable` with the reason.
Optional export failures leave `checkpoints/<incident-number>/unrecorded.json`
with a reason that replay reports, including when older partial stages exist.
Recording failures never discard completed resolution or learning metrics.
Keep the run directory private because exact agent
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

Responder and reflection replay run the real workers with the agent-script Codex
stand-in. Export converts live model I/O into `checkpoints/<request-digest>/script.json`
beside the exact task/result ProtoJSON. Use `--script <path>` to run an explicitly
converted or edited script through the same stage harness. Archived model I/O is converted in a temporary
directory on demand; the archived run stays untouched. Commands execute again against
captured Kubernetes state; captured command output is never substituted for execution.
Reflection file edits become script write/delete steps before worker validation and
real Go broker publication. Publication runs only in temporary clones of the source
repository, which must retain the checkpoint's accepted commit.

`broker` validates the saved worker result and file edits directly without running an
agent. Missing scripts, missing turns, unsupported recordings, or command assertions
fail explicitly. Archived task/result envelopes must satisfy the current strict
contracts; conversion of model I/O does not migrate retired request fields. There is
no model fallback or alternate replay registration.
Task/result equality and broker acceptance/rejection are reported separately: generated
session identifiers and timing can differ even when the broker verdict agrees.

Live registrations use a write-only `ModelRecorder`; model I/O, prompts, rollouts and
outcomes remain analysis evidence. Explicit registrations retain their own commands.
The recorder saves no command environment or auth files. Keep run directories private
and outside this repository. Hand-authored scripts use the same stand-in; see the
[testing guide](../../../../docs/testing-guide.md#hand-authored-agent-scripts-no-model).

# Tier 2a: toy real cluster

The reusable [toy application](../../../../apps/toy/README.md) has three tiny HTTP
services, real Deployments, local readiness probes and a frontend-to-backend
journey. It does not import or populate `third_party/sregym`. Pinned Calico
provides NetworkPolicy enforcement, required by the application network-policy incidents.
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
export TOY_RUN="$PWD/.fastloop/toy"
uv run python -m benchmarks.sregym.fastloop up --app toy \
  --run-dir "$TOY_RUN" --cluster-prefix my-toy- --dev-mount "$PWD"
uv run python -m benchmarks.sregym.fastloop run --run-dir "$TOY_RUN" --agent sdo \
  --stream tests/fixtures/sregym/fastloop/toy_latent_repeat.json \
  --reflection-session fresh --diagnosis-grading off \
  --timeout 300 --detection-timeout 75 --no-checkpoints
uv run python -m benchmarks.sregym.fastloop reset --run-dir "$TOY_RUN"
```

The cluster name uses `--cluster-prefix` plus `--worker-id`; creation refuses to adopt an existing
cluster without its environment record. The source mount is mandatory internally
and defaults to this worktree. `run` and `reset` rebuild the host operator, so code
changes need no runtime image rebuild. Generated responder and reflector registrations are the toy default. No model calls are needed.

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
The hotel tier also retires transient controller state and cached drift verdicts after stopping their writers.

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

<<<<<<< HEAD
## Editable agent turns

For arbitrary deterministic behavior beneath the real worker, use the versioned
JSON/YAML [agent script format](../../../../docs/testing-guide.md#hand-authored-agent-scripts-no-model).
It supports all four roles (plus source architecture), commands, SDO tools,
checkout file edits, bounded output assertions, and literal structured results.
`agent_script convert` turns successful Codex model-I/O recordings into editable
scripts. `agent_script registrations --out /absolute/registrations --image IMAGE`
wraps the production workers; pass that directory with `--agent-registrations`.
The same script is read from a local `SDO_AGENT_SCRIPT` file in Tier 1 and the
`sdo-agent-script` ConfigMap's `script.json` key in Tier 2. The testing guide
contains complete commands and the Bubblewrap prerequisite. Keep run scripts and
generated registrations outside Git; committed tests construct synthetic data.

## Activation continuity in toy results

Normal single-fault toy SDO runs keep detection active through reflection and
check it before baseline recovery maintenance. Each incident's
`memory_host_activations` lists every activation since injection, with accepted
commit, activation timestamp/latency, evaluation count before and after,
maximum evaluation gap, allowed gap, controller identities before/after, and
`controller_unchanged` / `passed`. The window covers build and catch-up latency
plus two allowed gaps on either side. The allowance is max(1 second, 5 times
`--time-scale`), compared with a nominal detector cadence of 2 times the scale.
Consecutive evaluation numbers and evaluations on both sides are required.
Missing expected activation logs, maintenance in the window, excessive gaps,
or a changed pod UID, pod/container start time or restart count fail the row
and run. An empty activation list means no activation occurred; it is not a
continuity proof. Scripted reflection proves plumbing, not model learning.

Harness `--timeout` (plus the single-fault verification margin) and reflection
waits use real wall time even with accelerated scripted detector timers. A state
read that exhausts the wait budget produces the stage timeout without launching
a subprocess with a 1 ms remaining budget.
=======
Astronomy seeds require the stock SREGym oracle observability stack before
application deployment. Fastloop `up` installs the chart and collector from the
pinned harness checkout; Helm dependencies are already vendored and are not
updated. For a source seed deployed directly through Go, first run:

```bash
uv run python -m benchmarks.sregym.adapter.oracle_observability \
  --kubeconfig "$KUBECONFIG" --cluster "$SREGYM_KIND_CLUSTER_NAME" \
  --sregym-dir third_party/sregym
```

Tracing Service declarations belong in the accepted application source; the harness does not rewrite them to
match the stock conductor's Jaeger ExternalName routing. The native benchmark
adapter also prepares this stack before installing a seeded controller. OpenEBS
is vendored at the revision recorded in the module; all telemetry runs inside
the lane's existing Docker CPU caps. Count the total cap across every kind node
under the host lease, including these infrastructure pods.

For a development source tree that needs additional cached Go module metadata,
set `SDO_DEV_MODULE_DOWNLOADS=1` and stage the matching download-cache files under
`/opt/sdo-dev/.fastloop/go-mod-download` on each node. The development Pod mounts
this directory read-only at `/go/pkg/mod/cache/download` and propagates the opt-in
to nested workers. Default mounts and offline network policy remain unchanged.

## Automatic repair defaults (GS1)

Every incident dispatches the responder with a playbook coverage view. `recorded-actions` is the default, with 10 response loops, 5 validation retries and a 5-minute verification timeout. The menu is on unless `SDO_RESPONDER_MENU=0`. The agent chooses mitigations and rollback; verified program counters are information only.

One trusted match is selected by distinct findings covered, then program ID and canonical bindings. Uncovered findings and unexplained drift do not prevent the attempt. Whole-incident verification still decides success; failure rolls back reversible writes and dispatches the responder sequentially with the attempt summary. No source-plan policy file is needed, even when the deployed application name differs from its benchmark name. See [automatic repair checks](../../../../docs/plan-safety-authority.md).

For a private scripted lane (no model calls), set `SDO_TOY_CLUSTER=gs1-toy` for `up --app toy --dev-mount "$PWD"` and use a fresh run directory outside the repository. `--cluster-prefix gs1-` selects an isolated toy cluster with the configured worker ID; `SDO_TOY_CLUSTER` remains an explicit override. Subsequent commands use the saved kubeconfig and environment. A target-port incident followed by a repeat exercises learning, save-time validation and agent-selected program reuse.

### Fresh memory and namespace control state

Non-toy SDO rows no longer retire control state selectively: the row-start wipe (see "Reused lanes")
deletes `<app>-sdo` entirely, so no onboarding record, controller checkpoint or worker ConfigMap from an
earlier attempt survives, and the install after it is always from-scratch on the planned commit. Only toy
lanes still run `stop` and `sdo-operator reset-control-state` before install. A new confirmed healthy install
establishes the next reachability reference; a legacy final record without reachability evidence logs
`reference_absent` and grants no reachability exclusions.

The legacy controller checkpoint is recognized by its exact name,
`sdo.dev/state-revision` annotation and `runtime-state.json` key. Retiring it
prevents restored controller state from resurrecting old onboarding authority.


## No-fault eval rehearsal

Before live slots on a prepared lane, run:

```bash
uv run python -m benchmarks.sregym.fastloop eval-dry-run --run-dir "$SDO_FASTLOOP_RUN"
```

Before touching the lane, the command imports every scripted registration's worker
module inside the lane's responder image (no network, seconds) and fails with the
build command for the `-scripted` variant when the image is a production build; the
result is `responder_image` in the report. It also reads the seed commit's `.sdo` (read-only) through the
scripted health judge's own seed loader and refuses a fresh, un-onboarded seed
(no full judge-owned health bundle): run the first
live row's production admission proof (`setup-proof.json`) instead. The result is
`seed` in the report. It then restores the app, installs and
onboards SDO with scripted registrations,
checks fresh-memory setup lineage and exact responder RBAC against the real installer,
requires fresh controller traffic clearance and qualified native prober scenarios,
checks benchmark probe placement, and captures the healthy baseline. Its final
pre-injection callback stops without injecting anything or invoking models.
`eval-dry-run/report.json` reports each invariant; all work shares a 300-second
process deadline (`--timeout` may lower it). Run a supported `reset` afterward
before transferring the lane or selecting live registrations.

Normal SDO fastloop runs enforce the same pre-injection admission: pending traffic
is not proof. Stabilization allows at most 300 seconds, one application-only restore
and one retry; its time is kept outside the injection clock. Restore selects each
object once from its sealed installed manifest, avoiding a later captured-spec patch.
A workload changing generation, Pod identity or restart count after Ready rejects
admission. SDO stays active while incident closure work remains pending.

An idle lane mounted from a retired checkout can be adopted explicitly with
`reset --dev-mount <worktree> --retarget-dev-mount`. After stopping writers and
restoring the app, reset copies the mounted source allowlist into node-local storage
and covers the old node mount; it never edits the old host checkout or changes images.
Subsequent resets refresh the snapshot. A recreated node must be reset before use.

Social reset replaces the post/timeline fixture as one unit after manifest
reconciliation. The original wrk job is suspended, post and timeline processes
are stopped, and all posts plus Mongo/user-Redis/home-Redis references are seeded
from ten deterministic posts for every original workload user. A complete
reference check must pass before clients and the same traffic job resume. Failed
seeding leaves writers stopped; another supported reset repeats the whole
fixture. This is benchmark setup only, never incident repair. The checker in
`benchmarks/sregym/fastloop/social_fixture.py` rejects empty fixtures, dangling
references and malformed int64 output; HTTP admission still runs independently.
Mongo fixture clients run in temporary, deadline-bounded Pods in the harness
namespace using the deployed Mongo image; they never share the database's
memory limit. They have no service-account token and are removed on completion
or failure. The application resource limits remain unchanged.
A supported abort stops SDO and benchmark transport, preserves the application
namespace, and saves the pre-abort native state beside the original state as
`*.abort-evidence.json`. Preserved applications are not reported as abort
leftovers; surviving SDO/transport resources still fail quiescence. Deliberate
application teardown must explicitly select `preserve_applications=False`.
Reset also handles namespaces absent after an older abort: it recreates only
the namespace named by the captured application baseline. Authorization/read
failures never become absence; system and SDO control namespaces are refused.

## Lanes on several hosts: one GitHub memory branch per app and run

`benchmarks/sregym/fastloop/remote_memory.py` shares an app's accepted memory between lanes on any
host through a private GitHub repository, with no shared filesystem. Every run must name the
repository and branch for each app in `memory-endpoints.json`; there is no default, and `main`/`master`
are refused (main holds only the README index):

```json
{"hotel-reservation": {"repo": "vibesys-playground/hotel-reservation-sdo", "branch": "live/20261028-mixed10"}}
```

Use `run_branch(label)` for `live/<yyyymmdd>-<run-label>`. Create the branch once from the app's seed
(`ensure_run_branch(endpoint, seed)`), and prepare every lane of that app from the same seed (or a clone of
the branch), so each lane's tree equals a branch commit. For each prepared idle lane, on its own host,
call `bind_lane_to_branch(environment, endpoint, seed=<first lane of the app>)` (the key defaults to the one
ssh offers github.com on that host; hosts differ). It checks the key first, pauses the controller, seeds the
branch from the first lane, makes every other lane adopt the branch history (its tree must equal a
branch commit), copies the host's existing GitHub key into the lane Secret `sdo-memory-upstream-ssh`
(mounted read-only; never printed) and recreates the controller with the upstream. `run_dir/memory-bind.json` lets a rerun finish a
binding that failed after the repository was replaced. Each lane keeps
its private PVC and a unique application name. The harness then treats it as a prepared shared
lane (no wipe, reseed, reset or reinstall): `fastloop.json` points `workspace` at a host mirror of
the branch that every shared-identity read and memory-head read refreshes from GitHub. The broker pushes and fetches directly; a lost push
race is replayed and checked again, and a conflict returns to reflection as `memory.conflict`, as
with one shared checkout. If GitHub is unreachable, lanes keep fixing faults and accept memory
locally; the next broker operation pushes it. At the end of the run, `close_run_branch(endpoint,
row, message=...)` tags `<branch>-final` and adds one README row on main.

## Parallel lanes with shared accepted memory

Use separate warm kind lanes for the same source application. Create each with
`up --parallel-root <local-directory>` as well as the ordinary `--dev-mount`, seed,
images, worker ID and problem flags. The local directory must use a filesystem
supporting `flock` (for example ext4/XFS on NVMe). Fastloop mounts it on every node
and preprovisions a lane-private `hostPath` repository PVC. An already provisioned
PVC or a lane created without that node mount must be recreated when idle;
attachment refuses to replace its storage. Keep experiment outputs outside the
source repository.

A plan is JSON, with absolute paths:

```json
{
  "run_id": "two-problem-proof",
  "provider": "codex",
  "model": "gpt-6-luna",
  "accepted_repository": "/local/parallel/accepted",
  "lanes": {"left": "/runs/left", "right": "/runs/right"},
  "encounters": [
    {"problem": "missing_configmap_hotel_reservation", "lane": "left"},
    {"problem": "k8s_target_port_user_hotel_reservation", "lane": "right"}
  ],
  "timeout_seconds": 3600
}
```

The plan-wide `provider` and `model` apply to preparation and every lane. They
default to `codex` and `gpt-6-luna`; use `claude` and `claude-sonnet-5-5` for a
same-model Claude proof.

Run `uv run python -m benchmarks.sregym.fastloop parallel --plan <plan.json>
--output <new-results-directory> --prepare`. The output must not exist yet; this
is checked before preparation. Preparation runs every unattached lane at once,
each in its own `parallel-prepare-lane` subprocess (log:
`<lane>/parallel-setup/<run_id>/prepare.log`). Each invokes the ordinary native
baseline/admission path without injecting a fault and syncs its still-private
accepted checkout. Image delivery and redeploys keep their host-wide limits (see
Host limits), and the lanes share one 8-job build budget. Wall time is the slowest
lane, not the sum. Then the first prepared lane is cloned as `parallel/accepted` if
absent. Every lane's accepted tree must equal a commit of the accepted first-parent
history, except per-cluster health captures (`discarded_cluster_captures`).
Differing learning must first be reconciled through the broker. Existing accepted
memory is never replaced. An accepted seed with initialized health memory avoids
independently authoring different initial health policies in the preparation stages.

The first cohort attaches behind one barrier, after every lane is prepared. It
pauses every controller through the recovery fence, stops all writers, binds only
their accepted `application` directory, and recreates the same native Jobs.
Proposal, session, capture, ledger and build roots remain private. Application
identity receives a lane suffix, making incident ID namespaces distinct. The
restricted controller group receives write access to the accepted checkout; Git
trust is scoped to its accepted mount. The external attachment report preserves
original Jobs, the cohort, each lane's matched accepted commit and the last phase
for recovery after an interrupted attachment. If some lanes fail preparation, the
prepared ones still attach and the command fails naming the others.

A running shared memory can grow. Run `parallel --prepare` with a plan whose new
lanes are unattached (already attached lanes in the plan are skipped). The new
lanes join as a later cohort. Only their own controllers pause and stop. Running
lanes keep publishing, and the live checkout is only read. A lane prepared before
peers published matches an older accepted commit, and its controller adopts the
current head when it resumes. Already attached lanes use `parallel` without
`--prepare`; `parallel-attach` performs only attachment for prepared idle lanes.

Each lane executes in its own subprocess, with explicit kubeconfig and capped
host build parallelism. Faults assigned to a lane stay ordered. All lane preflights
complete before any injection; each encounter uses the existing grade-at-closure,
strict-receipt, reflection-drain and paused-recovery paths. Seeding, archive
copy-back and `reset` are rejected after attachment. A lost host registry is
rebuilt only by inspecting the prepared running controller. A lost controller
requires explicit recovery; the runner cannot silently recreate its repository.

Output contains joined JSONL/CSV rows, the plan, per-lane logs/streams and a result
report that preserves every peer failure. Shared rows record the lane and accepted
commit ancestry across each encounter. Exact native request/outcome/reflection
commits remain in strict receipts. This runner has hermetic scheduling and broker
proofs; measure actual speedup and learning with a live pair before making either
claim.
Passing no-fault setup reports are reusable on an unchanged lane. A subsequent
`eval-dry-run` checks the content-bound receipt in `setup-proof/` before reset
or deployment. An exact match skips setup and reruns live admission; a miss
records the changed fingerprint components and performs the full rehearsal.
The live SDO `run` command also records its decision in `results/<run>/setup-proof.json`
and skips its deploy request only on the same exact match. Traffic, incident,
intake and resource/disk admission still apply at injection time. Changed code,
images, accepted memory, manifests, object UIDs/configuration or proof tools
invalidate the receipt, including changes made by reset. Unknown identity or
uncommitted source cannot authorize reuse. This setup receipt does not replace
the separate whole-learning-loop proof.


## Host limits

Every fastloop kind node (control planes included) gets Docker CPU, memory and
PID caps before CNI/image delivery. `up --cpu-limit 3 --memory-limit 16g
--pids-limit 8192` shows the defaults; use `--memory-limit 24g` for a lane that
needs more. The 16g default leaves headroom over Astronomy's recorded 9.6 GiB
p90 lane-container working set, which excludes node/page-cache overhead.
Memory plus swap is capped at the memory limit. Docker update failure stops
setup. Existing clusters are recapped when `up` attaches to them.

Image delivery (registry pulls into nodes and archive fallback alike) holds one
host-wide flock. Tier-2 namespace removal through app deployment holds one of
two host-wide redeploy slots; ordinary worker deploy requests use the same
slots. The default shared directory is `/var/tmp/sdo-fastloop/locks`,
outside the worker's private `/tmp`. For lanes under one experiment root, set
`up --host-lock-dir /absolute/experiment/root/locks --host-redeploys 2` for
**every** lane. These settings persist in the lane's worker environment.
Standalone image delivery accepts `--host-lock-dir` too. Environment defaults
are `FASTLOOP_HOST_LOCK_DIR` and `FASTLOOP_HOST_REDEPLOYS` (also supported by
older saved environments without these settings).

All callers must use the same lock directory and redeploy count. A capacity
file prevents callers silently disagreeing on the number of slots. To change
the count, stop all admissions first, remove only `redeploy.capacity` from the
shared lock directory, then start every caller with the new count. Keep the
lock files themselves: unlinking an in-use lock creates a second lock inode.
These caps apply when the updated harness is used; they do not modify lanes
that are already running an older harness.
>>>>>>> origin/vic-lsh/dev/sdo-improve
