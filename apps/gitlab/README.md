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
| `charts/objectstore` | app-owned | SeaweedFS S3 StatefulSet and Service `gitlab-objectstore`, the buckets (post-install hook Job), Secrets `gitlab-object-storage` and `gitlab-registry-storage` |

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
test and upgrade-only hooks are not deployed). Hooks (the upstream shared-secrets Job, the bucket Job) are ephemeral
install phases, not part of the accepted inventory (42 non-Secret objects, 5 hooks).

## Status of the live path

`fastloop up --app gitlab` builds the cluster, installs the prerequisites, commits the workspace and records the lane.
The last step, onboarding with the provided source, is blocked on a Go row: `host` has no `--deployment-plan` (a source
alone is not a plan: image declarations and expected workloads are needed too), and `gitlab_lane.onboard` refuses rather
than let the deployer agent author GitLab's deployment. `sdo-operator install-existing --deployment-source` works only
for a lane whose accepted memory already exists.

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
`SDO_GITLAB_CHART_ARCHIVE=<path to the pinned .tgz>` or `SDO_GITLAB_FETCH=1` is set (about 10 s). Both skip with a reason
when `helm` is missing and fail in CI.
