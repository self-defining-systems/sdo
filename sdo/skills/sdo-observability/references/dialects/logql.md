# LogQL patterns

For a source whose dialect is LogQL (a logs source). Detector queries over logs are bounded:
count and filter in the query rather than reading lines.

- Count, do not read: `sum by (pod) (count_over_time({namespace="<app-namespace>"} |= "level=error" [5m]))`.
- Select streams by labels the backend really indexes first, then filter lines; a bare line filter scans everything.
- Rate of a pattern per workload: `sum by (app) (rate({namespace="<app-namespace>"} |~ "panic|fatal" [5m]))`.
- Absence of an expected line (a periodic heartbeat) is a query that returns no stream; turn it into a value with
  a count compared with zero rather than relying on an empty result, which yields no finding.
- Keep label sets small; never put a request or user id in a selector.
- A source error or a warming source is a detector error, never a clear.
