---
schema_version: 1
generated_at_commit: hand-authored
generated_at: 2026-10-23T00:00:00+00:00
application: OpenTelemetry Demo Astronomy Shop
topology_fingerprint: hand-authored
---
# Astronomy Shop architecture

Hand-authored from the opentelemetry-demo Helm chart (chart 0.40.4, application 2.2.0) and the upstream application sources of that release (Envoy configuration, Next.js frontend API routes, Locust load generator); not produced by an SDO deployer round.

## Workloads and Services

Each enabled chart component renders a Deployment named after the component, and a ClusterIP Service of the same name when the component declares a service port. Pods carry `opentelemetry.io/name`, `app.kubernetes.io/name` and `app.kubernetes.io/component` labels equal to the component name.

- Entry: `frontend-proxy` (Envoy, port 8080) routes `/images/` to `image-provider` (port 8081), `/flagservice/` and `/feature` to `flagd`, `/jaeger/`, `/grafana/`, `/loadgen/` and `/otlp-http/` to the telemetry stack and load generator, and every other path to `frontend`.
- `frontend` (Next.js, port 8080) renders the storefront and serves `/api/...` routes that call `ad`, `cart`, `checkout`, `currency`, `product-catalog`, `product-reviews` (port 3551), `recommendation` and `shipping`.
- `checkout` calls `cart`, `currency`, `email`, `payment`, `product-catalog` and `shipping`, and publishes orders to `kafka`, which `accounting` and `fraud-detection` consume (neither has a Service).
- `recommendation` and `product-reviews` call `product-catalog`; `product-reviews` also calls `llm` (port 8000) for its AI assistant; `shipping` calls `quote`.
- Datastores: `postgresql` (catalog, reviews and accounting schemas, initialised from ConfigMap `postgresql-init`), `valkey-cart` (cart store), `kafka`.
- `flagd` serves feature flags from ConfigMap `flagd-config`; services fall back to flag defaults when it is unreachable.
- The chart also deploys `load-generator` and, through subcharts, the OpenTelemetry Collector, Jaeger, Grafana and OpenSearch. They observe or drive the shop and are not on its request paths.
