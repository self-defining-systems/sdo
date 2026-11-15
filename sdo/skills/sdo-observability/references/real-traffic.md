# Real traffic first

Real user requests are the first signal; probes add what traffic cannot show.

- Reuse what exists: the application's own metrics endpoint, exporters, alert rules, tracing and structured
  logs. Add instrumentation (source repair) only for what is missing.
- For every operation in the map, name the series that shows it failing: request count, errors and latency by
  route template, as the bound metrics source scrapes them (confirm each series exists,
  [telemetry](telemetry.md)). A series whose route label is empty or the same for every request
  cannot tell operations apart: find one that does, or record `missing` with the configuration or source patch
  that adds it. One line per operation under `## Request metrics` in the health-objective playbook:
  `<operation id>: <series and labels for requests, errors, latency>`. Note traces and error logs the same way.
