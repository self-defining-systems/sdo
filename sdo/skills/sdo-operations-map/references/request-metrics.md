# Request metrics and observe pointers

`onboarding init` writes the `entries` record and `observe.metrics` for each Service whose scraped pods expose a
request counter with a route label; edit only what it could not resolve (a counter name two Services share needs a
`selector`). `onboarding check --metrics` reads these fields. One request counter per entry Service goes under `entries`
(`labels` names the counter's route, method and status labels; `route` on an operation is the route label value
when it differs from `entry.path`). Each component's `observe` says, in a few words, where its metrics, logs and
spans are. A component that counts no requests (a static file server) gets `observe.logs` only and one reason per
red it cannot clear: `journeys-move-own-metrics` and `map-pointers` per operation, `routes-match-operations` per
exposed Service. This example passes the check (a test keeps it so):

```yaml
version: 1
components:
  - name: web                       # static files; no request counter
    workload: Deployment/web
    services: [web]
    holds_data: false
    observe: {logs: "container nginx: access log"}
  - name: api
    workload: Deployment/api
    services: [api]
    holds_data: false
    observe:
      metrics: "pod :8000/metrics, job=api"            # where to read its series
      logs: "container api: one line per request"     # container and what it logs
      trace_service: api                               # service name on its spans; omit when untraced
edges: []
entries:
  - service: api
    metric: http_requests_total     # the Prometheus counter name
    selector: {job: api}            # label values that pick this Service's series
    labels: {route: handler, method: method, status: status}
    errors: '5..'                   # RE2 over status values that count as errors
operations:
  - id: open-page
    writes: false
    entry: {service: web, port: 80, method: GET, path: /}
    path: [web]
  - id: list-items
    writes: false
    entry: {service: api, port: 8000, method: GET, path: /api/items/1}
    route: /api/items/{id}          # the handler label value
    path: [api]
reasons:
  - {item: journeys-move-own-metrics, subject: open-page, reason: "static file server; counts no requests"}
  - {item: map-pointers, subject: open-page, reason: "static file server; exports no metrics"}
  - {item: routes-match-operations, subject: web, reason: "static file server; counts no requests"}
probes: []
```
