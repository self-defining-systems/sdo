Synthetic traffic (health-judge owned). For an application that serves HTTP, also author
end-to-end synthetic traffic in the checkout under `.sdo/diagnostics/traffic/`. Ground it in the application source and
the accepted architecture summary: open the user-facing entrypoint's route handlers (its HTTP mux or
router) and derive paths it really serves, their methods, and parameter values its handlers accept, including the
records the application seeds at startup. Cover the main user journeys, read and write paths both when the
application has write paths. An isolated prober runs these journeys continuously at a few requests per second; the
traffic health detector opens an incident when a scenario violates its SLO, and a responder's repair is accepted only
when the same scenarios recover. Every scenario must therefore succeed against the healthy application.

1. Generators: Go package `generators` in `.sdo/diagnostics/traffic/generators/` exporting
   `func Scenarios() traffic.Catalog` (import "sdo.dev/operator/sdk/traffic"). Each traffic.Scenario has ID,
   Description, Target {Service, Port} (a source-backed Service and a port it exposes), DependsOn (the Services on
   the request path per the architecture summary, for localization), SideEffect (traffic.SideEffectRead,
   SideEffectIdempotentWrite, or SideEffectWriteWithCleanup with Cleanup steps), Marker (writes only: a string such
   as "sdo-synthetic" that appears in every write request and names dedicated synthetic data), Steps, and Detects
   (extra fault classes: traffic.FaultWrongBody when a step checks the body, traffic.FaultSlow). A simple step is one
   line: `{Name: "list", Endpoint: traffic.GET("/items", traffic.Params{"from": traffic.DateRange("2024-01-01",
   "2024-01-31"), "to": traffic.DaysAfter("from", 1, 3)}).Contains("expected text")}`. Other parameter
   generators: Const, OneOf, IntBetween, FloatBetween, FromSeed(count, render), FromSeedPair(key, count, first,
   second) for matching credentials, FromPrevious(key); chain steps with .SaveJSON(field, key) and "{key}" path
   segments. Use .Contains(...) whenever the application reports failures inside a successful status. Generators only
   build requests and check responses: never import net, os, or the clock, and draw randomness only from the rng the
   engine passes. Never read or modify real users' data beyond the fixtures the application's own source seeds.
2. Workloads: a file `.sdo/diagnostics/traffic/workloads/<name>.yaml` with apiVersion sdo.dev/v1alpha1, kind
   TrafficWorkload, name equal to the file name, purpose, ratePerSecond, and scenarios [{id, weight}]. Write
   `health` (purpose health-probe, ratePerSecond at most 4, read scenarios only unless a write is idempotent) and
   `verify` (purpose verify-burst, ratePerSecond about 12, duration 3s, the same scenarios). Optional: timeout (2s),
   slo {window, minSamples, maxErrorRate, maxTimeoutRate, latencyPercentile, maxLatency}, and journey workloads
   (purpose journey, bounded duration) for writes that should not run continuously.
3. Firing policy: a traffic-health detector's window is re-evaluated on every probe poll (about every 500ms), far
   faster than the 2-evaluation firing threshold, so a brief data-plane stall (observed around 3s, roughly 1 in
   6-10 pod-network changes on a small cluster) can otherwise reach it and dispatch a responder for nothing.
   `traffic.NewDetector` therefore defaults `Persistence.MinDuration` to `traffic.DefaultHealthMinDuration` (9s) for
   any health-class detector that leaves it unset, so a scenario must violate its SLO continuously for that long,
   not just reach the evaluation count, before it fires; real faults still fire within it plus about one poll. Do
   not set `Persistence.MinDuration` back to a lower value for a traffic-health detector.
4. Link reachability: a user journey can stay healthy while one of the services it depends on has become impossible
   to reach afresh, because the caller keeps using connections it opened earlier. For each service-to-service
   dependency the application source declares (a client dial, a configured peer address, an environment variable or
   config value naming another service and port, and the architecture summary's dependency edges), add one link to
   workloads/links.yaml: apiVersion sdo.dev/v1alpha1, kind TrafficWorkload, name `links`,
   purpose `link-probe`, and `links: [{from: <calling Service>, to: <called Service>, port: <the port the called
   Service exposes>}]`. Both names must be source-backed Services in the deployer handoff and the port the one the
   caller really uses, taken from the source, not guessed. Do not declare links to datastores or services the
   application does not run itself. Optional: interval (default 1s), timeout (default 1s), failures (default 5, the
   consecutive failed fresh TCP dials that report an edge). No scenarios and no generator code are involved; the
   controller installs the detector, which decides deterministically from the prober's dials and names the failing
   edge. Do not encode any particular cause of an unreachable edge.
Omit synthetic traffic only for an application that serves no HTTP.