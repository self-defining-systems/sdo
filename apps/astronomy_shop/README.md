# Astronomy Shop

OpenTelemetry Demo **2.2.0**, with application source in the pinned `upstream/`
submodule and the SREGym-compatible Helm chart in the pinned `charts/` submodule.
`pins.json` records both revisions and the reference harness revision. The demo's
`llm` component is a local mock service; this deployment requires no model API.

The checked-in `kubernetes/astronomy-shop.yaml` is plain Kubernetes source for
SDO's existing manifest strategy. It preserves the reference chart's images,
24 Deployments, OpenSearch StatefulSet, collector DaemonSet, service names,
labels, ports, database initialization, telemetry and `flagd-config` flags.
The release and namespace are `astronomy-shop`; the storefront is
`frontend-proxy:8080` and the workload control endpoint is `load-generator:8089`.
As in SREGym, bundled Prometheus and browser workload traffic are disabled.

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
