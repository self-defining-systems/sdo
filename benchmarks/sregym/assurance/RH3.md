# Mixed10 controller rehearsal

Phase A supplies hand-written repair directives for the first ten entries of the
seed-42 reliable mixed-app stream. `adapter/scripted_replay_catalog.py` records the
exact IDs and ordered actions. `scripted_codex/mixed10.py` binds them to the existing
scripted Codex directive format. The existing adapter scripted worker and scripted
Codex responder both execute `adapter/scripted_replay.py`; there is no second
controller, repair broker, receipt implementation, or harness recovery shortcut.

Each live mutation runs through `sdo act`, which captures pre-apply evidence and
writes action receipts. `sdo source` supplies accepted rendered manifests, including
trusted deployed images. Full-field JSON patches remove injected fields. Service
repairs preserve Kubernetes-allocated addresses. Missing ConfigMaps are recreated.
The proposal checkout records a source-restoration annotation on each restored
manifest; credential repairs also persist `secretKeyRef` declarations. The Go
broker alone publishes these source edits. No Secret data is read or printed.

The credential candidate points product-catalog, accounting and product-reviews at
the existing rotated connection Secret and restarts product-catalog. It does not
reset PostgreSQL to an old password. The poison-record candidate discovers the
failed offset from consumer logs, stops the consumer, advances its group past that
record, and resumes it without deleting topics. These astronomy candidates are
not oracle-proven: RH1 stopped on injection/oracle infrastructure before repair.
RH2's social repairs are recorded oracle successes, still without native receipts.

Slot 9 restores the three accepted Deployment specs, removes the webhook and DNS
policy, restarts social-graph-service, temporarily selects Recreate for
url-shorten-service, waits for the old anti-affinity Pod to disappear, and retries
its pending replacement Pod. After current-generation
rollout convergence it restores the accepted strategy. Recovery also restores the
seed; failure remains an error rather than a fabricated successful action.

## Phase B prerequisites

Keep the accepted seed's private deployment ledger with its Git history. Set
`SDO_SEED_BROKER_ROOT` to the independently produced seed broker directory before
installing a fresh rehearsal controller. The adapter passes this trusted host
path as `install-existing --seed-broker-root`; Go validates the bounded named
image evidence against accepted history and restores only `source-evidence/`
after resetting the PVC. Proposals, incident receipts and other private state
are not imported. A Git-only seed can start with a current lock but loses its
image authority after the first source repair. Do not infer that authority from
rendered locks or healthy live Pods.

The experiment lead must first refresh the social seed (JG1, lane 53) and astronomy
seed (PO1, lane 52). Each fastloop environment must have a green full detector
baseline, functioning official oracles, full-recovery support, and trusted scoped
image-input evidence. Its controller/responder images must include this PR and
carry the scripted image label (build with `assurance/build_images.sh`). The
wrapper refuses an ordinary model image, the wrong lane/namespace, an expired or
missing `responder-lanes` lease, or a busy shared lane lock. Use a disposable copy
of each refreshed seed, not the immutable original.

Run one command per slot, with SLOT from 0 through 9 and RUN_DIR/LANE_LOCK selecting
the matching prepared environment and shared lock (52 for astronomy, 53 for social):

```bash
source /mnt/data/shli/cache/env.sh
uv run python -m benchmarks.sregym.assurance.mixed10_rehearsal \
  --slot "$SLOT" --run-dir "$RUN_DIR" --lane-lock "$LANE_LOCK"
```

Add `--dry-run` to print the directive and runner arguments without touching a
cluster, Docker, coordination state, or the environment. Real execution checks the
lease, calls `coord gate`, resets to seed, generates a single-entry stream outside
the repository, and invokes the existing fastloop stream runner. It uses scripted
responder/reflection registrations, trust threshold 1, full cleanup and an official
faulty-oracle check while the controller is paused. Oracle errors are invalid
injections, never fault proof. `pre-repair-oracle.json` retains this benchmark-only
evidence. Diagnosis grading is off; no model is invoked.

After checking the scripted-image labels, the wrapper creates a missing control
namespace and an empty credentials Secret for its required runtime volume. It
does not read Secret data or alter an existing Secret. Credential slots 1 and 4
also require two full cleanup passes and a passing recovery oracle after each.

The scripted health-refresh registration retains the accepted complete generic
bundle only when the objective and selected deployed resource identities match
the accepted lock. It edits no detector policy. The existing typed judge-task
wrapper and independent Go broker validation still own acceptance. New topology,
a changed objective or an incomplete bundle stops the rehearsal.

After the runner completes, the wrapper requires the official mitigation oracle,
full-cleanup verification, strictly parsed native receipts, broker source and
outcome commits, and a zero-gap accepted rendered lock bound to the repaired source tree. A stale
seed lock is rejected even if it has no gap entries. It preserves evidence under
`RUN_DIR/results/rh3-slot-.../`, then resets to seed even after a failed run.
An expired lease or failed host gate aborts before reset/injection; rerun only once
the experiment lead has renewed the lease or host load permits it.

The replay waits up to 600 seconds for current observedGeneration and the desired
replicas/updated/ready/available counts, with no unavailable replicas. It then polls
`sdo verify` for up to 600 seconds before returning. The wrapper supplies a 3600-second response/composite budget
for the sequential waits and mutation overhead. The native controller's default
30-minute response deadline contains RH2's measured 512-second attempt; its
120-second post-response verification window starts only after the responder has
finished these waits. The older adapter repair path's 120-second rollout watcher
is raised to 600 seconds with an outer 610-second subprocess bound, covered by a
fail-first regression representing the 170-second clone plus 41-second delay.

Run `bash scripts/test_fastloop_hermetic.sh` for the no-cluster gate. It includes
the replay catalog, slow rollout, pre-repair oracle, and real `sdo` CLI action
receipt regressions alongside the existing real Go controller/broker/tool-transport
gate. Passing Phase A does not establish injection validity, native receipt
success, lock continuity or full seed recovery for any real stream slot.
