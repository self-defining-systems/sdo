# Application telemetry

Application telemetry is what the application reports (metrics, logs, traces) and the backends that store it. It is
not the controller's own firing telemetry. The task's `telemetry` block (mode, sources, capabilities) is the
authority; this page says how to use it. The validator rules are listed in [rules](rules.md#telemetry-bindings).

## Reading the task

- `mode`: `open` (you may bind sources and backends), `pinned` (the environment's sources are exactly the ones
  listed; query them and add detectors, write nothing under `telemetry/`), `floor` (no application telemetry; the
  Kubernetes API and SDO probes only, so omit telemetry detectors).
- `sources[]`: id, kinds, adapter, dialect, default, capabilities, origin, endpoint and, when offered, a `publish`
  block. The dialect names the query language of that source: [PromQL](dialects/promql.md), [LogQL](dialects/logql.md).
- `capabilities[]`: cluster reads the installer granted (`pod-discovery`, `node-proxy`, `pod-logs`). You may rely on a
  listed one; you can never create or widen one.
- Responders get the same block read-only. A needed binding is recorded for the next designer pass, not edited.

## Operator-provided sources

A listed source with origin `operator` (the task line says `operator-provided`) is already bound by the environment.
Name it by id in detector queries (or omit `Source` when it is the default of its kind); never copy it into
`sources.yaml` (redefining it is `telemetry.origin`, a second default is `telemetry.default`). When the listed sources
answer what you need, write nothing under `.sdo/diagnostics/telemetry/`. Go installs the generic `metrics-alerts`
detector only when an effective source has the `alerts` capability; keep it as Go wrote it.

## Detector queries

A query names its source (`Source`, omitted only for the default source of its kind) and carries text in that
source's dialect; the runtime passes it through unparsed. Name only a source the task lists or you bind in the same
commit. An unbound, ambiguous or unreachable source is a detector error that names the source; it never clears.

## Binding a source (open mode only)

`.sdo/diagnostics/telemetry/sources.yaml`, strict YAML, no duplicate keys, at most 16 entries:

```yaml
version: 1
sources:
  - id: prom-extra            # ^[a-z][a-z0-9-]{0,30}$, not an operator-provided id
    kinds: [metrics]          # kinds the adapter serves: metrics, logs, traces
    adapter: prometheus       # a registered adapter; the error lists them
    endpoint: http://prom-extra.<control-namespace>.svc:9090
    default: false            # at most one default per kind across all sources
    credential: ""            # a granted credential name, never a value
    ready: {text: "up", min_results: 1}
    publish:
      scrape: {annotations: true}
      otlp: {metrics: http://prom-extra.<control-namespace>.svc:9090/api/v1/otlp/v1/metrics}
```

- The endpoint is a Service in the control or application namespace (`<svc>.<ns>.svc`) or a host the environment
  allowlisted; never an IP literal, `localhost` or a URL with userinfo, query or fragment.
- `origin` is assigned by Go; do not write it.
- `options` are decoded strictly by the adapter; keys it does not define are an error.
- There is no `apiVersion`, `kind`, `name`, `url` or `config` key, and `alerts` is a capability, not a kind.
- Run `sdo-operator memory check`: it applies the broker's policy and the same registry.

## Backends and recipes

`.sdo/diagnostics/telemetry/backends.yaml` selects an environment-offered recipe for a backend SDO deploys in the
control namespace. A source names it with `backend: <id>`.

```yaml
version: 1
backends:
  - id: prom-stack            # same grammar as a source id
    recipe: prometheus        # an offered recipe; the error lists them
    uses: [pod-discovery]     # cluster capabilities: pod-discovery, node-proxy, pod-logs
    params: {retention: 6h}
```

`uses` lists only capabilities the environment granted and the recipe declares. Go applies the recipe objects; free-form
manifests, RBAC and cluster-scoped objects are not accepted.

## Instrumentation

Instrumentation is a source repair a responder commits or a deployer captures as a patch, never an edit of application source by
you. Use the source's `publish` block: annotate pod templates for scraping when `scrape.annotations` is set, or send
OTLP HTTP to its full metrics URL. A deployment overlay under `overlays/telemetry/` may carry the annotations. Do not
infer publishing support from a query endpoint.
