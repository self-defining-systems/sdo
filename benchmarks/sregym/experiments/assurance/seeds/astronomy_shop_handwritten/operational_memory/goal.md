---
schema_version: 1
owner: human
application: OpenTelemetry Demo Astronomy Shop
---
# Health objective

The deployed Deployments named accounting, ad, cart, checkout, currency, email, flagd, fraud-detection, frontend, frontend-proxy, image-provider, kafka, llm, payment, postgresql, product-catalog, product-reviews, quote, recommendation, shipping, valkey-cart remain available; the deployed Services named ad, cart, checkout, currency, email, flagd, frontend, frontend-proxy, image-provider, kafka, llm, payment, postgresql, product-catalog, product-reviews, quote, recommendation, shipping, valkey-cart expose ready endpoints (accounting and fraud-detection are Kafka consumers without Services); the ConfigMaps flagd-config and postgresql-init and every non-optional ConfigMap volume reference remain present; no NetworkPolicy denies all traffic to or from these workloads; and representative shopping requests through frontend-proxy succeed. The load generator and the telemetry stack (OpenTelemetry Collector, Jaeger, Grafana, OpenSearch) are not part of this objective.
