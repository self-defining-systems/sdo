# Request metrics (generic detector)

`memory.PrepareHealth` installs the generic `request-metrics` detector (`sdk/requestmetrics`) when a bound source
answers range queries and the app map has operations; do not author it. It reads each entry's request metric through the
map's `entries` labels and each operation's `route`, and reports per operation: at least 5 failed requests, at least 5%
of its requests and 5 times its healthy share over 30 s, on two consecutive 5 s steps (server errors, and 4xx only as a
jump over the healthy 4xx share). It runs in shadow mode (`detection.request_metrics_shadow`, default on): findings are
clues in `sdo detect` that name the operation and route, and never open an incident. It also judges, with the same rule, route values in the entry's metric that no declared operation covers (busiest 20 per entry; a finding says when the list was truncated) and marks those findings "undeclared route", so an incomplete map does not hide a failing route. An operation whose entry lacks the
metric, route or status label is reported as `request-metrics.not-instrumented`; fix the map's `entries` or the
application's instrumentation instead of ignoring it.

