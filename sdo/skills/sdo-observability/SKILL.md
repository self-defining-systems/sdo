---
name: sdo-observability
description: Make every user operation observable and query it. Covers reusing metrics, tracing and logs, one request metric per entry with route, method and status labels, and queries.
---

# Observing an application

Real user requests are the first signal; probes add what traffic cannot show. SDO tells operations apart only
by what the application reports, so each operation needs its own request, error and latency series.

## Reuse first

- Look for what exists before adding anything: a metrics endpoint, exporters, scrape annotations, alert rules,
  tracing SDKs and structured logs. Read the source and config, then confirm live.
- `sdo sandbox list` prints each environment's Prometheus and tracing URLs. Query them directly
  ([PromQL](references/dialects/promql.md), [LogQL](references/dialects/logql.md)).
- Change code only for what is missing, and keep the change small: a middleware, an exporter, a label.

## One request metric per entry

Each exposed entry (Ingress backend, Gateway route, LoadBalancer, NodePort Service) and each internal entry
(background workers, schedules) gets one `entries` record in the map ([map](../sdo-operations-map/SKILL.md)):
the request metric, a selector, the names of its route, method and status labels, and an error pattern. SDO
builds every operation's series from it:

- requests: `sum(increase(<metric>{<selector>, <route>="<route>", <method>="<method>"}[w]))`;
- errors: the same with `<status>=~"<errors>"`.

So the labels must tell operations apart:

- The route label carries the route template (`/api/orders/:id`), never the raw path (unbounded values) and
  never one value for every request. A shared value such as `unknown` or an empty label fails the check: find
  the label that differs, or fix the instrumentation so it does.
- Method and status labels come from the request, not from configuration.
- A worker's metric counts jobs by queue or job name and outcome; its errors pattern matches failed outcomes.
- Confirm each series in Prometheus after real or probe traffic: `sdo journeys run OP --on sandbox-N` shows each
  operation's series before and after a run.

## Traces and logs

- Traces cross every service hop with propagated context, so a slow or failing hop is visible by service. Reuse
  the app's tracing; else add an OpenTelemetry SDK exporting to the environment's collector.
- Error logs are structured (JSON or logfmt) and carry the route or job name and the error, one event per line.

## Querying metrics (responders too)

- Rates: `sum by (route) (rate(<metric>{...}[5m]))`; error ratio: errors over requests by the same labels.
- Compare a window before the incident with the current one; a missing series is an unknown, never a clear.
- Detector queries name a bound source ([telemetry](references/telemetry.md),
  [metrics sources](references/metrics-sources.md)); an unreachable source is an error that names it.

More: [real traffic](references/real-traffic.md).
