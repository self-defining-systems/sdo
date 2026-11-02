# Astronomy Shop health seed (hand-authored)

`operational_memory/` is a complete `.sdo` tree for the opentelemetry-demo Helm chart 0.40.4 (application 2.2.0) that SREGym deploys (namespace `astronomy-shop`, entry `frontend-proxy:8080`). It was written by hand from the chart and the upstream 2.2.0 sources (Envoy configuration, Next.js API routes, Locust file, service handlers), not by an SDO lifecycle round, and contains no fault, verdict, feature-flag or injection knowledge.

- `health-objective`: required shop Deployments (21) and Services (19; `accounting` and `fraud-detection` have none), ConfigMaps `flagd-config` and `postgresql-init`, scale-to-zero, unavailable replicas, selector mismatch, missing ready endpoints, and NetworkPolicies that deny all ingress or egress. The load generator and telemetry subcharts are excluded.
- Traffic (`health`, `verify`), all reads: `storefront-home`, `product-browse`, `product-recommendations`, `product-reviews`, `ads`, `cart-view` (a dedicated synthetic session), `currency-conversion`, `shipping-quote`, `product-image`. Checkout, add-to-cart and the AI assistant are writes or call the LLM, so `checkout`, `payment`, `email`, `kafka`, `accounting`, `fraud-detection` and `llm` are covered only by the objective detector and the link probe.
- `links`: 20 edges from the chart's `*_ADDR`/`*_HOST` environment and the Envoy clusters; datastores excluded.
- `service-endpoints`, `traffic-health`, `traffic-links` are the installer's generic packages, checked in byte for byte. Regenerate after an installer change with `SDO_UPDATE_ASSURANCE_SEEDS=1 go test -count=1 ./memory/validate/ -run TestAssuranceSeedsPassHealthJudgeCheck` from `sdo/operator`.

## Overlap with the benchmark grader's probe

The composite grader (`sregym/conductor/problems/composition/grading.py`, `ENTRY_POINTS["OpenTelemetry Demo Astronomy Shop"]`) requests `/`, `/api/products`, `/api/products/OLJCESPC7Z`, `/api/recommendations?productIds=OLJCESPC7Z` and `/api/data?contextKeys=telescopes`. Overlapping scenarios: `storefront-home` (`/`), `product-browse` (`/api/products/<id>`, OLJCESPC7Z among ten seeded IDs), `product-recommendations` (`/api/recommendations?productIds=<id>`) and `ads` (`/api/data?contextKeys=<category>`, telescopes among six). The seed does not request the product list `/api/products`; its other five scenarios have no grader counterpart.

The generic `pod-admission` detector requires successful server-side dry-run Pod creation in the application namespace, even while existing Pods are Ready. API rejection is unhealthy; transport failure is an observation error.
