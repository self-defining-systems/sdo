# Request metrics (generic detector)

`memory.PrepareHealth` installs the generic `request-metrics` detector (`sdk/requestmetrics`) when a bound source
answers range queries and the app map has operations; do not author it. It reads each entry's request metric through the
map's `entries` labels and each operation's `route`, and reports per operation: at least 5 failed requests, at least 5%
of its requests and 5 times its healthy share over 30 s, on two consecutive 5 s steps (server errors, and 4xx only as a
jump over the healthy 4xx share). Its error findings page: a failing operation or route opens an incident (`detection.request_metrics_shadow`, default off) and the page names the operation or route with failed and total counts; recovery waits on the same finding clearing. Not-instrumented findings are clues only. It also judges, with the same rule, route values in the entry's metric that no declared operation covers (busiest 20 per entry; a finding says when the list was truncated) and marks those findings "undeclared route", so an incomplete map does not hide a failing route. An operation whose declared entry lacks the
metric, route or status label is reported as `request-metrics.not-instrumented`; fix the map's `entries` or the
application's instrumentation instead of ignoring it. An operation whose entry service has no `entries` record (the
app exports no request metric for it) is not applicable: no finding, never a failed acceptance or an incident, only a
coverage note ("request metrics not applicable to operation ...") that the operability report shows under
`O3.detector-coverage`. Declare an `entries` record when the app does export the metric.

A raised error finding does not clear because a route went quiet: it stays until a later 30 s window has at least 5
requests and none failed (the detector looks back 5 minutes), and says "Insufficient recent traffic to confirm recovery"
meanwhile; incident recovery waits on the same finding, so a low-volume route cannot read as recovered. Each error finding
also carries up to 3 recent failing requests (method, path with record ids, status, time) read from the entry service's pod
logs through the recorded logs source (JSON access-log lines with a status field, and common/combined text lines;
query strings are dropped). When none can be read the finding says why (no log result, no pod logs, lines not parseable,
no failing line for this route). The samples appear in the page and in `sdo detect`.
