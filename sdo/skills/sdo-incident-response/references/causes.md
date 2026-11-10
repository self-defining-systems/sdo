# Reporting causes

Each confirmed root cause carries live `evidence`, the `explained_detectors` it accounts for, and optional
`static_context`. Static files (scripts, manifests, ConfigMap bodies) belong in `static_context`, never in live cause
evidence.

Set `cause_class` to the mechanism, from the schema's list (for example `network-delay`, `network-loss`,
`config-value-wrong`, `data-corrupt`, `state-lost`, `capacity-exhausted`, `code-defect`); leave it empty only when
none fits.

Set `reach`:

- `in-scope`: a change in the application namespace (a live repair or a source patch) can fix it.
- `out-of-reach`: you localized the cause, but nothing in the namespace can fix it (a node, the network path, an
  external service, cluster policy).

For an out-of-reach cause, cite the opening finding that shows it as `detector-finding` evidence whose `source` is
its detector ID, and do not repair around it: restarting, rolling back or scaling unrelated workloads does not fix
it and can cause harm. When health does not return, SDO records the outcome `escalated` and routes the cause to a
human. An escalation is never a success and teaches no playbook.
