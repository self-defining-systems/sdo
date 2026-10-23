# Astronomy Shop

OpenTelemetry Demo **2.2.0**, with application source in the pinned `upstream/`
submodule and the SREGym-compatible Helm chart in the pinned `charts/` submodule.
`pins.json` records both revisions and the reference harness revision. The demo's
`llm` component is a local mock service; this deployment requires no model API.

The checked-in `kubernetes/astronomy-shop.yaml` is plain Kubernetes source for
SDO's existing manifest strategy. It preserves the reference chart's images,
24 Deployments, OpenSearch StatefulSet, collector DaemonSet, service names,
labels, supported ports, database initialization, telemetry and `flagd-config` flags.
The release and namespace are `astronomy-shop`; the storefront is
`frontend-proxy:8080` and the workload control endpoint is `load-generator:8089`.
As in SREGym, bundled Prometheus and browser workload traffic are disabled.

The renderer applies a measured memory policy to every regular and init container:
`limit MiB = max(stock MiB, 128, 64 * ceil(2 * observed_peak_bytes / (64 * 2^20)))`.
The pinned observations and policy constants are in `memory-baseline.json`. Peaks
combine 90 minutes of Prometheus total usage/working set with live cgroup v2
`memory.peak` on luna-pilot-52 under ten Locust users, browser traffic disabled.
Completed init containers have no retained samples and receive the 128 MiB floor.
A new container requires a baseline entry before rendering. Effective memory
requests are preserved explicitly: Kubernetes otherwise defaults an omitted request
to the increased limit. Containers previously without a memory request or limit
retain a zero request. Existing CPU requests and limits are preserved. Existing limits larger than the rule are never lowered.

The stock limits leave little headroom on this cgroup v2 lane: frontend-proxy
repeatedly OOMKilled at 65 MiB; Kafka, Grafana, collector and OpenSearch approach
or reach their limits. Total charged memory includes page cache (for example,
accounting and OpenSearch), so RSS alone understates the required budget.
The nodes each have a three-core quota while Envoy sees 64 online CPUs. This
can affect worker sizing and scheduling; no causal GC claim is made without a
controlled comparison. The traffic configuration matches the pinned harness.

The renderer also retains the collector Prometheus reader on port 8888 and
removes OpenSearch's unsupported Performance Analyzer port 9600; HTTP and
transport remain on 9200/9300. Images, application flags and detectors stay pinned.

**Benchmark comparability:** stream slots 6 and 8 use
`valkey_memory_disruption` with `noeviction`. Its injector floods unbounded
1 MB values from ten threads, with no configured Valkey `maxmemory` below the
cgroup limit. Raising valkey-cart from 20 to 128 MiB changes its memory-fault
threshold and may change time to failure. Those draws are memory-dependent;
revalidate injection before comparing results. Slot 4's ad manual-GC draw also
warrants comparison caution after increasing the ad limit, although it triggers
explicit GC rather than exhausting a configured memory bound. Slot 0's oversized
shipping request is CPU-based; requests remain unchanged.

Changed limits from AS2 (MiB; `none` means previously unlimited):

| Workload | Container | AS2 | New |
|---|---|---:|---:|
| otel-collector-agent | opentelemetry-collector | 200 | 448 |
| grafana | grafana-sc-alerts | 100 | 256 |
| grafana | grafana-sc-dashboard | 100 | 192 |
| grafana | grafana-sc-datasources | 100 | 192 |
| grafana | grafana | 175 | 384 |
| accounting | accounting | 256 | 384 |
| accounting | wait-for-kafka (init) | none | 128 |
| ad | ad | 300 | 576 |
| cart | wait-for-valkey-cart (init) | none | 128 |
| checkout | checkout | 20 | 128 |
| checkout | wait-for-kafka (init) | none | 128 |
| currency | currency | 20 | 128 |
| email | email | 100 | 192 |
| flagd | flagd | 75 | 128 |
| flagd | init-config (init) | none | 128 |
| fraud-detection | fraud-detection | 300 | 576 |
| fraud-detection | wait-for-kafka (init) | none | 128 |
| frontend | frontend | 250 | 256 |
| frontend-proxy | frontend-proxy | 65 | 192 |
| image-provider | image-provider | 50 | 128 |
| kafka | kafka | 600 | 1152 |
| llm | llm | none | 192 |
| load-generator | locust-exporter | 64 | 128 |
| payment | payment | 140 | 256 |
| postgresql | postgresql | 100 | 256 |
| product-catalog | product-catalog | 20 | 128 |
| product-reviews | product-reviews | 100 | 192 |
| quote | quote | 40 | 128 |
| valkey-cart | valkey-cart | 20 | 128 |
| opensearch | opensearch | 1100 | 2240 |
| opensearch | configfile (init) | none | 128 |

Initialize the two source submodules from the repository root:

```bash
git submodule update --init apps/astronomy_shop/upstream apps/astronomy_shop/charts
```

Deploy into the selected Kubernetes context:

```bash
kubectl create namespace astronomy-shop --dry-run=client -o yaml | kubectl apply -f -
kubectl apply --server-side --field-manager=sdo-deployer -f apps/astronomy_shop/infrastructure/
kubectl apply --server-side --field-manager=sdo-deployer -f apps/astronomy_shop/kubernetes/
kubectl -n astronomy-shop rollout status deployment --timeout=600s
kubectl -n astronomy-shop rollout status statefulset/opensearch --timeout=600s
kubectl -n astronomy-shop rollout status daemonset/otel-collector-agent --timeout=600s
kubectl -n astronomy-shop port-forward service/frontend-proxy 8080:8080
```

For SDO, select only `apps/astronomy_shop/kubernetes` as the manifest path, or
`kubernetes` when preparing a standalone source checkout. Cluster roles and
bindings are installed separately from `infrastructure/`. They retain the chart's
collector discovery permissions. No SREGym code or metadata is needed at runtime.
The versioned image tags match SREGym; SDO must record independently observed
container digests during deployment to publish a complete rendered lock.

To regenerate, use Helm 3.19.5 and the chart's locked dependencies:

```bash
helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts
helm repo add jaeger https://jaegertracing.github.io/helm-charts
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo add opensearch https://opensearch-project.github.io/helm-charts/
helm dependency build apps/astronomy_shop/charts/charts/opentelemetry-demo
uv run python apps/astronomy_shop/render.py
```

The renderer fixes the Kubernetes API version, validates source revisions and
Helm version, and assigns the application namespace explicitly. Render twice
and compare bytes to check reproducibility. Public demo configuration is included
by the upstream chart; never use it as a production credential configuration.

Image build declarations and Dockerfile-specific scope files are described in
[scoped application image inputs](../BUILD_INPUTS.md). Prepare those scope files
before fresh builds and trusted deployment evidence collection.
