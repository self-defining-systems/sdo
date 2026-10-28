# Warm-lane experiment tools

Use `check-plan` to admit every selected fault before lane work; the
[fault observation tools](../faults/README.md) describe its independent probes.
`preflight` checks the host and frozen images. `run` owns incident injection,
native learning, independent grading and fenced recovery. `sequence` adds a
restartable journal for adjacent first/exact-repeat pairs with private lane memory.

## Change the application without recreating kind

`up --no-dev-mount --reuse-cluster-only` attaches using the existing lane's
`KUBECONFIG`. Missing cluster, kubeconfig or incompatible node topology stops the
operation; this mode never falls back to cluster creation.

`retire-app` combines a bounded old-app retirement with attach-only deployment:

```bash
uv run python -m benchmarks.sregym.fastloop retire-app \
  --run-dir OLD_ENVIRONMENT --next-run-dir NEW_ENVIRONMENT \
  --next-namespace social-network --next-problem SOCIAL_PROBLEM_ID \
  --seed FRESH_SOCIAL_SOURCE --lease LANE_LEASE --coord COORD_COMMAND \
  --controller-image CONTROLLER --responder-image RESPONDER --validator-image VALIDATOR
```

It checks the current lane lease before writes, refuses unfinished native closure
or workers, pauses an idle native controller, deletes only the old application
namespace and its SDO namespace, and waits for both to disappear. The new app is
deployed on the same cluster and kubeconfig through attach-only `up`. Completion
requires the ordinary `lane_resources` check for the new app. The next environment
retains intent and completion receipts; a partial retirement can be retried with
the same fixed target set. The command never removes infrastructure or unrelated
namespaces. New-app model credentials must be provisioned through the experiment's
existing credential helper; retirement does not read or copy credential data.

## Lane admission and transient baseline observations

`check-plan --plan PLAN --lane-environment RUN_DIR` filters the plan against the
saved lane's topology and emits `runnable_problem_ids` plus a `lane_plan` decision
for every problem. Decisions with `status: not_runnable_on_lane` include the
unsatisfied injector prerequisites. `run` applies the same admission before any
wipe, worker startup or onboarding, and writes `results/RUN_ID/lane-plan.json`.
Worker count comes from the recorded `SREGYM_KIND_WORKER_NODES`; old lanes without
that fact cannot admit faults requiring a worker count. Optional `lane_topology`
in `fastloop.json`, captured by `up` using node metadata, APIService availability
and Calico configuration, records worker/control-plane counts and confirmed cluster
features (`network-policy`, `metrics-server`, `calico-bgp`). Missing feature
proof fails closed. SecretRotation injectors remain excluded until the lane guard
allows their Secret read. This admission never grants production repair authority.

Baseline probe construction retries the complete observation for 90 seconds on
`ObservationUnavailable`, discarding partial references between attempts. If
exhausted before any injection, the result has `infra_error: never_injected_retry`.
A sequence writes `never-injected-N.json` and `retry.json`; restarting it uses a
fresh `-retry-N` result directory and preserves all earlier evidence. Missing,
uncertain or attempted-injection evidence still holds for native recovery.

## Shared image delivery

All lanes publish frozen host image IDs to the harness-owned
`sdo-fastloop-registry` container on the `kind` Docker network. The existing
host-wide image-delivery lock covers registry creation, publication, node pulls
and retention. A content-derived tag locates a manifest; verified cache hits skip
`docker push`. Nodes pull its immutable manifest digest and retain the original
source image names. Manifest bytes, the frozen config ID and each node's CRI
image ID are checked before reporting success. Identical images under different
source names share the same publication.

Lane teardown removes only retired lane-owned registries. It leaves the host
registry and its data intact for other lanes. The host registry is bound to a
loopback port for uploads; nodes reach it through the kind network. An existing
container with that name must prove harness ownership. Registry failures retain
the existing loud, recorded archive fallback.

Each `image_delivery.jsonl` entry includes `registry` metadata: container name,
`uploads`, `cache_hits`, `publication_seconds`, `pull_seconds`, `frozen_ids` and
`manifest_digests`. Total `seconds` also includes setup and retention; use the
phase fields to distinguish a first publication from subsequent lane delivery.
