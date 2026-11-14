# Generic Kubernetes object evidence

`program.ReadView.Object(ref)` reads a private unstructured object by exact API
version, kind, namespace and name. Cluster objects use an empty namespace. Errors
mean unavailable or absent evidence; do not turn them into healthy verdicts.
`program.FieldValue(object.Object, "/spec/field")` accepts JSON pointers, including
array indexes and escaped map keys. Existing typed reads and field paths work.

A generic repair uses `program.SetField` with a JSON pointer path and an existing
provenance value (`FromSource`, `FromObserved`, or `FromParam`). The API server
validates live resource schemas. Identity and controller write metadata cannot be
patched this way. Cross-namespace and cluster repairs require an exact finding
reference. Return plans; programs never receive a writer.

Snapshots carry generic objects in `objects`; old typed fixtures still load.
The same objects support diagnosis and mitigation replay, including new CRDs,
PVCs, webhooks and foreign namespaces. Read evidence directly from Object rather
than replaying a stale primitive answer after a simulated patch. Secrets are
excluded from discovery capture; Secret data must never enter fixtures.
ConfigMap opaque values remain key-only. Structured JSON retains operational
fields with credential-named entries redacted; boolean strings are retained.
Captured errors, redactions and size/discovery failures are evidence gaps.

Discovery is the capture inventory. New API kinds require no capture or replay
switch cases. Reinstall/update controller read grants after introducing new APIs.

For generic operational observations, `ReadView.KubernetesRead` exposes read-only API
requests (including metrics), and `ReadView.Observe` returns the exit code of a
container command or HTTP request. These are recorded through the shared read
view; missing/failed observations remain Unknown. Keep commands deterministic and
free of LLM calls, and reference credentials only inside the container. Mutation
belongs to a mitigator plan, using the
[general action vocabulary](../../sdo-playbook-authoring/references/general-actions.md).
Do not declare a fault-specific SDK gap when a general primitive covers the operation.
