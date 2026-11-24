# Experiments on EKS lanes

From zero to a graded SREGym learning plan or a release series on EKS vcluster lanes. Platform: [docs/eks.md](../../../../benchmarks/lanes/docs/eks.md); verbs, build and `lanes manager start`: [quickstart](../../../../benchmarks/lanes/docs/quickstart.md); `plan.yaml`: [plan.md](../../../../benchmarks/lanes/docs/plan.md). `lanes` below is the root's binary: `ssh <manager-host> 'cd <root> && ./bin/lanes-main-<sha9> <verb> ... --root .'`, where `<root>` is the lanes root directory kept in the external sdo-exp directory.

## 1. Prerequisites

1. **Quiet manager machine.** Every EKS step (harness, `go run`, kubectl, aws) runs on the manager's machine; under load rows fail `not-injected` and a series loses its lane. Run the manager on one machine (root above, host `aws-vc`); `lanes preflight` check `manager-machine` must pass. `/home` and `/mnt/data` are per host: checkouts, inputs, plan dirs and every sdo-exp path below live on the manager's host, so build, generate plans and analyze there. Tools: the shared tools `bin` directory, kubectl in `/usr/local/bin`.
2. **AWS.** Source the `devops-agent/env.sh` kept in the external sdo-exp directory; it sets the profile (`/mnt/data/shli/.aws-sdo`), region and `red`/`awsr` (redact 12-digit ids). Check credentials without printing: `aws sts get-caller-identity >/dev/null && echo ok`. Never print an account id, registry host or key; pipe aws and docker output through `red`. vCPU quota: `awsr service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A`.
3. **Host cluster.** `lanes host adopt aws-vc` makes or attaches `sdo-host-vc1`. Its `system` node group holds every vcluster control plane, about 6 per m6i.xlarge (`EKS_LANES_PER_SYSTEM_NODE`): `ceil((lanes + 1) / 6)` nodes. A lane's up raises it; adopt never lowers it under running lanes.
4. **Lane nodes.** Sized from the workload footprint net of 2 GiB per node. The `sdo-lane` EC2NodeClass carries kubelet `kubeReserved` 600Mi, `systemReserved` 400Mi and `evictionHard` memory 750Mi / pid 10%, so a heavy app loses a pod, not the node; nodes made before that keep none (drift budget 0). SREGym rows use `lane: {cpus: 8, memory: 28Gi}`, the release series `30Gi`.
5. **`hosts.yaml`:** skeleton and pitfalls in [eks-hosts.md](eks-hosts.md): budgets, `max_lanes`, `vcpu_quota`, the load gate, `workload_env` (reseed switch, `FASTLOOP_HOST_REDEPLOYS` equal to the host-lock capacity), `FUZZOPS_BIN`, `hook_command`, `checkouts`, lane labels.
6. **Images.** Build on the quiet host. In a detached checkout of the pin (submodule initialized, `uv sync --extra test`): `SDO_IMAGE_TAG=eks-real-<sha8> bash scripts/build_sdo_images.sh` (plain images, real agents). Stub rows name no image: build `SDO_SCRIPTED_WORKER=1` under the tag the host env's `SDO_IMAGE_TAG` sets (else `v0.1.0`). The stub-agent release series pins main plus a stub-registrations commit and builds `SDO_IMAGES=controller`. Those registrations route every agent through the scripted stub worker, so a pool on that pin can never run a real agent (a fault row there dispatched, made one call with 0 tokens and fixed nothing): pin plain main for a series with real agents, and read the session's `agent-resolved.jsonl` provider after its first dispatch. Pre-push once so lanes do not push the same layers:
   `TAG=eks-real-<sha8>; R=$(aws sts get-caller-identity --query Account --output text).dkr.ecr.$AWS_REGION.amazonaws.com; aws ecr get-login-password | docker login -u AWS --password-stdin "$R" 2>&1 | red; for i in sdo-controller sdo-sregym-responder; do aws ecr create-repository --repository-name sdo/$i >/dev/null 2>&1; docker tag $i:$TAG $R/sdo/$i:$TAG && docker push -q $R/sdo/$i:$TAG 2>&1 | red; done`

## 2. SREGym with learning

Run these from a checkout of a pin that has `benchmarks/sregym/lanes/learning_plan.py`.

0. **Inputs** (`<root>/inputs/sregym-eks`): `instances.json` (`{"ids": [...]}`, the problem ids), one base memory per app (`uv run python -m benchmarks.sregym.lanes.baseline REPO COMMIT <inputs>/baseline-<alias>` from the onboarded app repository `SREGYM_LANES_SEED_<app>` names) and the credentials hook.
1. **Problems** need a valid stub row on an EKS vcluster lane first; node-access problems need a full-cluster lane (the fault-matrix note in the external sdo-exp directory). Prove new ones with a `--stub` plan (`valid` true). A problem the stub cannot undo (admission webhooks) gets one real-agent p1 row instead.
2. **Plan.** `uv run python -m benchmarks.sregym.lanes.learning_plan --name NAME --pin SHA --host aws-vc --inputs <inputs> --out DIR --app hotel=hotel_reservation:<inputs>/baseline-hotel --problem hotel=<problem> ... --tag eks-real-<sha8> --auth-file <ADC json> --vertex-project <project> --warmup --pools --parallel <lanes>`. Arms: `w` (untimed warm-up, equal to p1 so it can claim the pool lane), `p1` (learns), `cold`, `p2` (seeded from p1's memory). Real agents run Sonnet on Vertex with `after_playbook: agent`; cold and p2 set `learning.closing_turn: false`. `--timing-from <previous>/final-speed.json` sets the row deadline to 2.5x the measured medians (the default deadlines make a stuck row wait very long).
3. **Reseed.** `SREGYM_LANES_RESEED=1` makes later rows on a lane run the warm transaction instead of a fresh install. `fallback` in the `reseed` column means a failed warm attempt plus a full reset.
4. **Pre-warm and run.** `lanes add DIR/pool-<alias>.json` per app; wait for every pool lane `ready` in `lanes status`; `lanes plan add DIR/plan.yaml --dry-run`, then without it (clock start). Watch `lanes plan status NAME` at most every 10 minutes; read the first row of each kind early ([campaign-operations.md](campaign-operations.md)).
5. **Results.** `lanes plan results NAME --tsv --final --grade-fields valid,success,problem,page_source,agent_dispatched,resolution_path,reseed,reseed_reason,page_to_healthy_seconds,responder_usd,reflection_usd,learned,seeded`. Speed: `uv run python -m benchmarks.sregym.lanes.row_speed --root <root> --plan NAME --out <sdo-exp dir> --lanes-bin <root>/bin/lanes-main-<sha9>` writes `final-speed.md` (stages, wall against floor, per-arm medians) and `final-speed.json` (the next `--timing-from`).
6. **Worked examples** (sdo-exp): `eks-lanes/sregym/REAL-REPORT.md` (learning, p2 against cold), `eks-lanes/sregym/fast/FINAL-SPEED.md` and `FINAL-SPEED-2.md` (speed, what remains).

## 3. The release series

Read the [commit-series README](../../../../benchmarks/commit_series/README.md) and the [replayer README](../../../../benchmarks/common/replay/README.md) first.

1. **Data.** `uv run python -m benchmarks.commit_series.gitlab build DEST --pack <fuzzops>/examples/gitlab --cache DIR` (the series file and id of earlier runs are in the external sdo-exp directory). Baseline at the series base: `... gitlab baseline SRC DEST --series series.json`; use the newest baseline for the series from the external sdo-exp directory (an older baseline can flip a release's verdict). Before any live attempt: `uv run python -m benchmarks.commit_series.preflight --series series.json --baseline DIR`.
2. **Pool and claim.** A hold pool (`hold`, `keep_lanes`, `selector`, `settings.source_branch` with `up`, `down`, `rewind`) builds one warm lane; plan specs `claim` it. A new hooks pin needs a new pool (the workload path is part of its profile).
3. **Plan.** One arm, one item, `reps: 2`, `parallel: 1`; rep 2 runs on rep 1's lane reset to the golden copy, and position 1 proves the reset (`verify_start`). That holds only if one pool lane carries the selector label. A worked plan is in the external sdo-exp directory.
4. **Inner loop** on a warm debug lane, outside the manager: `source` the lane's `env.sh` from the commit-series directory of sdo-exp, `cd` to the hooks checkout, `uv run python <sdo-exp>/commit-series/dbg/drive.py 3 --jump` (base to position 3 as one diff), then `drive.py 4`. `dbg/repeat.sh LANE N` runs N rounds plus a reset (`./tp-hook.sh reset`), logging to its `progress.log`; it hard-codes a hooks checkout and positions 3-4, so edit it per pin. Shake flaky verdicts out there before a full run.
5. **Verdicts.** `lanes plan results NAME --tsv --grade-fields outcome,deploy_seconds,incident_opened,agent_tasks`: one row per release. The run reports are in the external sdo-exp directory.

## 4. The iteration loop

Read [iteration.md](iteration.md) and [campaign-operations.md](campaign-operations.md). Keep one warm debug lane per app, fix from your branch, rerun only the failing row as a one-item experiment there, and run the full plan only to confirm. The manager refuses a dirty checkout (`workload was built from a modified checkout`): commit each fix in the dev checkout (same path), register its sha in `checkouts:`, one experiment per fix; same path means no rebuild. Python-only fixes reuse images; Go or responder changes need a new tag.

## 5. Coordination and safety

- One driver per effort, logging in its sdo-exp log; others on the root announce there.
- Never restart a shared manager while another plan runs or a lane is in cluster-up. When `lanes status` shows nothing in flight, `lanes manager restart`; run `lanes manager start` if the unit stays down.
- Rehearse in a private root with its own manager.
- Verbs reject unknown flags; `lanes add` has no `--dry-run` (only `plan add`).
- Check spend in `lanes status` before every add; stay under `budget_total`. The `idle-lane` alarm (plan `alarms.idle_lane`, 30m) and `idle-ceiling` mean lanes bill without work. Pause a pool (`lanes pause <pool>`) once its lanes are claimed so it stops refilling.

## 6. Teardown

Through the manager: `lanes plan remove NAME`, `lanes remove <pool>` per pool, `lanes lane release <lane>` per lane. Confirm with `lanes status` and `awsr eks list-clusters` (only the host cluster; its system group scales to 0 with the last lane). Then `disk-janitor.sh --dry-run`, check, run without the flag; it keeps live pins and `disk-janitor.keep`.

## 7. Known open issues

- **Detectors "still running a previous evaluation".** Alerts stop but Recovered never holds; the row ends `fault still active` and the lane goes down. No fix yet: save the controller log and incidents (earlier cases are in the run reports in the external sdo-exp directory), rerun on a reset or rebuilt lane, and do not count it as an SDO result.
- **Per-cluster worker lock.** Pre-starting the row's second SREGym worker in the warm transaction fails `ClusterInUseError: cluster <lane> is already in use`. Do not pre-start workers.
- **Corrupt buildx cache** (`missing parent ... bucket: not found`, `frontend grpc server closed unexpectedly`). With no image-input change since the last good tag, retag that image; else `docker buildx prune -af`, then rebuild.
