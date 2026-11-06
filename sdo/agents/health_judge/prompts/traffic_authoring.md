Synthetic traffic (detection-designer owned). For an application that serves HTTP, write the probes under
`.sdo/diagnostics/traffic/` (full API: references/traffic.md of the `sdo-detector-authoring` skill). Derive paths,
methods, parameters and expected response content from the entry point's real route handlers and the records
the application seeds. An isolated prober runs every health-probe workload continuously; its traffic detector
opens an incident when a scenario violates its SLO, and a repair is accepted only when the same scenarios
recover. Every scenario must succeed against the healthy application and fail on every sample when what it
covers breaks.

1. Generators: Go package `generators` in `.sdo/diagnostics/traffic/generators/` exporting `func Scenarios()
   traffic.Catalog` (import "sdo.dev/operator/sdk/traffic"). Each traffic.Scenario has ID, Description, Target
   {Service, Port} (the operation's entry Service and port), DependsOn (the Services of every component on the
   probe's map path), SideEffect (traffic.SideEffectRead, SideEffectIdempotentWrite, or SideEffectWriteWithCleanup
   with Cleanup steps), Marker (writes and setup steps: a string of at least 6 characters naming the test
   identity; it appears in every write request), Setup (steps that create the scenario's own test identity or
   fixture once per prober start; end each with .OrExisting(status, text) for the duplicate answer; never rely on
   data set up by hand), Steps, and Detects (traffic.FaultWrongBody when a step checks the body,
   traffic.FaultSlow). A step is one line, for example `{Name: "list", Endpoint: traffic.GET("/items",
   traffic.Params{"page": traffic.IntBetween(1, 3)}).Contains("expected text")}`. Other parameter generators:
   Const, OneOf, IntBetween, FloatBetween, DateRange, DaysAfter, FromSeed(count, render), FromSeedPair(key, count,
   first, second), FromPrevious(key); chain steps with .SaveJSON(field, key) and "{key}" path segments; send a
   body with .WithForm(params) or .WithJSON(template, params). Check exact content, never only the status:
   .JSONEquals(path, "{key}") for a value the probe wrote, .ExactSet(path, field, values...) for the probe's own
   collection, .Each(path, field, pattern) for every member of other data, .Contains(...) only for text pages; log
   in as the test identity (.SaveCookie(name, key), then .WithHeader("Cookie", "name={key}")), never with a token
   forged from application secrets. Generators only build requests and check responses: never import net, os,
   or the clock, and draw randomness only from the rng the engine passes.
2. Workloads: files `.sdo/diagnostics/traffic/workloads/<name>.yaml` with apiVersion sdo.dev/v1alpha1, kind
   TrafficWorkload, name equal to the file name, purpose, ratePerSecond and scenarios [{id, weight}]. Write the
   health-probe workloads `probe-call`, `probe-write` and `probe-read-later` (one per probe kind, each
   scenario in exactly one) only for a kind that has at least one scenario: a workload file with no scenario is
   invalid, so omit that kind's file (and its probe-kind rows in the map) when the application has none, for
   example no `probe-read-later` without a data-holding component on a write path. Also write `verify` (purpose verify-burst, ratePerSecond about 12, duration 3s, the call and
   read-later scenarios that exist). Optional: timeout (2s) and slo {window, minSamples, maxAge, maxErrorRate,
   maxTimeoutRate, latencyPercentile, maxLatency}; set maxLatency per scenario to three times its slowest healthy
   iteration under load, at least 1s (memory check notes each one left on the default as
   `traffic.default-latency`). Journey workloads run only on demand and never feed detection.
3. Firing policy: `traffic.NewDetector` defaults `Persistence.MinDuration` to `traffic.DefaultHealthMinDuration`
   (9s) for a health-class detector, so a brief data-plane stall does not dispatch a responder while lasting
   failures still fire. Do not set it lower.
4. Links: a journey can stay healthy while a dependency cannot be reached afresh, because callers reuse open
   connections. For each map edge between Services, add one link to workloads/links.yaml: apiVersion
   sdo.dev/v1alpha1, kind TrafficWorkload, name `links`, purpose `link-probe`, and `links: [{from: <calling
   Service>, to: <called Service>, port: <the port the caller uses, from the source>, protocol: <how the caller
   talks to it>}]`. Both names must be Services in the manifest inventory. Include the application's own data
   stores, not services it does not run. Protocols: http (optional http.path, http.statusMin/statusMax), grpc
   (grpc.tls when required), redis, memcached, mongodb, postgres or mysql; tcp only when none fits. A link proves
   the server answers its protocol, not that queries succeed. Optional: interval (1s), timeout (1s), failures
   (5 consecutive failed fresh probes report the edge). `sdo-operator memory check` notes each link no
   health-probe scenario drives (`traffic.undriven-edge`); drive it, or write a YAML comment above the link
   saying why no synthetic request can safely cross it.
Omit synthetic traffic only for an application that serves no HTTP.
