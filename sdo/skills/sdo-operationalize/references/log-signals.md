# Log signals

A log signal watches the lines a component writes when it fails: a credential its peer rejects, a volume it
cannot write, a dependency it cannot reach. Probes see the user's symptom, often only a timeout; the component's
own log usually names the cause. A log signal also catches work that stops without any request failing, such as
a worker that no longer logs its jobs.

Write one file per signal at `.sdo/diagnostics/logs/<signal-id>.yaml` (lowercase, digits, dashes). Go installs a
`logs-<signal-id>` health detector for it when you run the memory check: it fails like any health detector, and
an incident closes only when it clears. Do not write detector code for it.

```yaml
components: [repo-server, api]   # map components; every container of each one's workload is read
window: 2m                        # judgement window, 30s to 5m (default 2m)
signals:                          # failure lines
  - id: auth-rejected
    re2: 'permission denied: wrong (hmac|signature)'
    min_count: 2                  # at least this many in the window (default 1)
expect:                           # activity the healthy component always logs
  - id: job-done
    re2: 'Job \S+ done'
```

How it judges (only against the healthy application, never against zero):

- The controller samples each window while the application is healthy and settled. Until it has, nothing is
  judged, and a controller restart starts the samples afresh.
- `signals`: fires `log-signal.lines` when a window's count across the component's Pods reaches `min_count`
  and is more than twice the most any healthy window logged. A line the healthy application also logs at that
  rate never fires.
- `expect`: fires `log-signal.absent` when every healthy window logged at least `min_count` of it and a
  complete window of a running workload logged none. Use it only for activity the healthy application logs
  steadily (a periodic job, a request log under probe traffic).
- Reads are bounded (64 KiB per container per cycle; capped counts are lower bounds). A failed read is a
  detector error, never a clear. A finding carries counts, the window and at most one short redacted sample
  line, never stored values.

When to write one (keep it to a few, each tied to code that logs the line):

- For each component on a user operation, read its source and docs for the errors it logs when a dependency,
  credential, volume or peer fails (`log.Error` calls, error strings returned to a caller and logged, fatal
  paths). Write one pattern per failure the source names, not one per log line; match the stable text, never
  an ID, address or timestamp.
- The client side too: the caller usually logs a failed dependency (a timeout, a refused or rejected call)
  more plainly than the server.
- A worker or scheduler that logs each unit of work: an `expect` pattern.

Bounds: 1 to 8 components and 1 to 8 patterns per file, at most 48 patterns across all files; each pattern is a
valid RE2 that does not match an empty line. Confirm each pattern on the live application before you finish: a
`signals` pattern should match nothing or rarely while healthy (read the component's logs), and an `expect`
pattern should match every window.
