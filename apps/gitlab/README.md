# GitLab lane

GitLab 19.5 (upstream Helm chart `gitlab/gitlab` **10.5.4**, app v19.5.3, CNG images from `registry.gitlab.com`) deployed
and operated by SDO on kind. The harness provides the deployment source; SDO's trusted deployer renders it with the
generic helm renderer and applies it. No agent authors Kubernetes configuration for this app, and SDO contains no
GitLab-specific code. Everything GitLab-specific is here and in `benchmarks/sregym/fastloop/gitlab_lane.py`.

`pins.json` records the chart (version, URL, sha256), the datastore images (digest-pinned), the ingress controller
manifest (URL, sha256, images), the Helm and Kubernetes versions the renders were checked with, and the namespace and
release (`gitlab`, `gitlab`). `tests/unit/benchmarks/sregym/fastloop/test_gitlab_lane.py` fails if the chart values drift
from the pins.

## One deployment source: an umbrella chart

SDO has one `DeploymentSource` per app and no chain of sources, so the whole app is one helm source, the umbrella chart
`chart/` (`gitlab-lane`):

| subchart | origin | content |
| --- | --- | --- |
| `charts/gitlab` | upstream, **fetched at lane setup, never committed** (11 MB unpacked, includes its disabled subcharts) | the unmodified chart |
| `charts/postgresql` | app-owned | PostgreSQL 17 StatefulSet, Service `gitlab-postgresql`, Secret `gitlab-postgres-password` |
| `charts/redis` | app-owned | Redis 7.4 (password) StatefulSet, Service `gitlab-redis`, Secret `gitlab-redis-password` |
| `charts/objectstore` | app-owned | SeaweedFS S3 StatefulSet and Service `gitlab-objectstore`, the buckets (a regular Job: the registry needs its bucket to become ready), Secrets `gitlab-object-storage` and `gitlab-registry-storage` |

The upstream chart 10.x no longer bundles PostgreSQL, Redis or MinIO, hence the app-owned datastores. Their object names
are fixed (not release-derived) because `global.psql.host` and friends name them. Credentials are synthetic and valid only
for the throwaway lane (`chart/values-kind.yaml`). `chart/values-kind.yaml` is the kind values file (SDO `value_files`):
keys under `gitlab:` are the upstream chart's values (`gitlab.gitlab.webservice...`), `global:` reaches every subchart.
It turns off everything cluster-scoped (cert-manager, bundled ingress, Gateway API, Prometheus, runner, Zoekt, OpenBao,
AI gateway, KAS), uses ingress class `nginx`, domain `127.0.0.1.nip.io`, one replica everywhere and a 2 Gi Gitaly volume.

`assemble.py` builds the source workspace (never in the repo):

```bash
uv run python apps/gitlab/assemble.py <empty-dest>                     # downloads, verifies and unpacks the pinned chart
uv run python apps/gitlab/assemble.py <empty-dest> --archive gitlab-10.5.4.tgz   # offline, same verification
```

The result has `chart/` (umbrella plus `charts/gitlab`), and `assemble.deployment_source()` is the matching ProtoJSON
helm `DeploymentSource` (`chart_path: chart`, `value_files: [chart/values-kind.yaml]`).

The vendored chart is never edited. SDO's renderer handles it as shipped (disabled subcharts leave the release set, the
test and upgrade-only hooks are not deployed). Hooks (the upstream shared-secrets Job) are ephemeral
install phases, not part of the accepted inventory (the bucket Job is a regular workload).

## The provided deployment plan

`fastloop up --app gitlab` builds the cluster, installs the prerequisites, commits the workspace, builds the plan and
onboards through `host --deployment-plan` (no deployer agent; architecture and the health judge still run).
`gitlab_lane.provided_plan(workspace)` renders the committed chart through SDO's production `render-source` and derives
the plan from that render, nothing is maintained by hand:

- `source`: the helm source above (`chart`, `chart/values-kind.yaml`).
- `images`: one `prebuilt` image source per image repository of the rendered containers (no tag, no digest), so a
  release that only bumps tags still matches; SDO pins each rendered tag to a digest at deploy. Hook-only images (the
  upstream shared-secrets and bucket Jobs) are not in the accepted inventory and fall to the default prebuilt source.
- `expected_workload_ids`: the Deployments, StatefulSets, DaemonSets and Jobs of the render (hooks are not objects of
  the main phase), as `Kind/gitlab/name`, in the stable form Go proposes: the migrations Job, whose name carries a hash
  of its images, is `Job/gitlab/gitlab-migrations-*`, so every release's render matches.

Known blocker: the helm policy on main (`render/helm/internal/policy`) rejects the real upstream chart (`tpl`,
`duration` and a computed `include` in its helpers), so the real-chart tests below fail until that policy or a lane
workaround changes. The synthetic stand-in passes.

## Releases and the followed upstream

`releases.json` lists the chart releases of the 10.5 line (plus the 10.4.2 baseline before it), each with the archive
URL, sha256 and what it changes against the release before it: changed image references, added or removed values keys,
values schema, added or removed workloads, object count. The changes were measured by rendering both releases through the
umbrella chart (the `derivation` field says how). `fastloop up --app gitlab --chart-release 10.5.0` starts the lane at
any listed release (`assemble.py` also declares that release as the umbrella dependency); the default is `pins.json`.

The lane follows a Git ref (`--source-kind git`): `fastloop up` serves a Git remote in `gitlab-upstream` (the toy's
server), seeded with the workspace commit.

`fastloop gitlab replay --run-dir <lane>` (`benchmarks/sregym/fastloop/gitlab_replay.py`) replays the releases after the
chart the lane's accepted source holds: per release it replaces `chart/charts/gitlab` with the verified archive, bumps the
umbrella dependency, commits and pushes to the followed ref, then waits for SDO's outcome (advanced, held, repaired,
incident) and records agent tasks, wall time and human edits (0). It starts only on a steady lane (green operability report,
no incident) and stops on a hold that no agent role can act on. The record format is in the analyze-experiment
`operability-report.md` reference. Typical: `fastloop up --app gitlab --chart-release 10.4.2 ...`, onboard, run the generic
fault stream, then `fastloop gitlab replay`.

## Cluster prerequisites (the environment's job)

Installed by `fastloop up --app gitlab` (`gitlab_lane.install_prerequisites`), not part of the deployment source:

- ingress-nginx `controller-v1.12.1` (kind provider manifest, sha256-verified from `pins.json`) with IngressClass `nginx`;
  its two images are delivered to the nodes like the SDO images. The ingress hostnames are
  `gitlab.127.0.0.1.nip.io` and `registry.127.0.0.1.nip.io`; reach them with a port-forward to the controller (the lane does
  not map host ports).
- Default StorageClass `standard` (kind), outbound access to `registry.gitlab.com`, Docker Hub (datastores) and
  `registry.k8s.io`: about 11 GitLab images (several GB) are pulled by the nodes. Mind Docker Hub rate limits.
- Not required: metrics-server (HPAs pin min=max=1, unknown metrics are not fatal), cert-manager, Gateway API CRDs.

## Resource needs

Requests (measured from the render): upstream chart about 0.55 CPU / 1.9 GiB, datastores about 0.3 CPU / 0.6 GiB, ingress
and kube-system about 1 CPU / 1 GiB. A cold start (database migrations, image pulls) peaks near twice that. Coordination
board claim for one lane: **6 cores, 12 GiB memory, 40 GiB disk** (`gitlab_lane.RESOURCE_CLAIM`); the kind node limits
(`--cpu-limit`, `--memory-limit`) must be raised above the toy defaults (suggest `--cpu-limit 5 --memory-limit 10g`).

## Hermetic checks

`tests/unit/apps/test_gitlab_workspace.py` (go_integration): a synthetic stand-in chart with the pinned name and version
(disabled subchart with a CRD and a random function, an install hook, an upgrade-only hook, a test hook) renders through
SDO's production `render-source` to the same non-hook objects as `helm template`. The real chart runs only when
`SDO_GITLAB_CHART_ARCHIVE=<path to the pinned .tgz>` or `SDO_GITLAB_FETCH=1` is set (about 10 s); with it, the real
chart's plan is also built and printed (`-s`). Both skip with a reason
when `helm` is missing and fail in CI.
