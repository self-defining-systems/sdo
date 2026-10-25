# General operations and replay evidence

Choose a general primitive before declaring an SDK gap. The operation inventory
and executable floor live in `docs/sdk-action-inventory.{md,json}`. API RBAC is
the execution boundary. Programs propose plans; they never receive a writer.
Credentials remain container environment references. Never read Kubernetes Secret
data, resolve credentials in the controller, or include them in commands or
recordings. Environment-backed authentication headers use `HeaderEnv`; `Bearer`
is an environment reference. Plain `Headers` carry nonsecret values only.

## Kubernetes objects and subresources

- `PatchObject{Ref, Type, Patch}` supports `json`, `merge`, and `strategic` patches.
  JSON patch covers add/remove/replace/move/copy/test, including array insertion.
  Strategic merge uses the registered built-in schema; use JSON/merge for CRDs.
- `PatchObject{Ref, Edits}` supports list elements by identity. Each
  `ListEdit{Path, Match, Op, Value}` selects a JSON-pointer list, matches all fields
  in `Match` exactly once, and removes, replaces, or inserts before that element.
  Missing/ambiguous matches leave the object untouched. Raw patch and edits are
  exclusive. JSON patch `/-` appends, including to an empty list.
- `ApplyObject{Ref, Object, Mode}` creates a missing named object or merges desired
  fields into an existing object. `create` requires absence; `replace` replaces
  its document. `DeleteObject` deletes any named nonsecret object. General
  operations cover cluster and foreign scopes through discovery and API RBAC.
- `KubernetesRequest{Ref, Method, Path, Query, ContentType, Body}` reaches the API,
  including eviction, server-side apply and other subresources. Supply `Ref` for
  measured object effects. Server-side apply uses its API content type and
  field-manager query. For reads, use `ReadView.KubernetesRead` (GET/HEAD).
  The controller never reads TokenRequest credential responses; use a container
  command when the generated credential must be consumed inside the container.
- `WaitFor{Ref, Path, Equals, Timeout, Interval}` polls an actual JSON-pointer
  value. Defaults are 30 seconds / 250 milliseconds. Keep timeouts bounded by
  the incident deadline. A missing or failed observation never proves readiness.

API mutations retain preimages and direct observations. Partial batches compensate
reversible object writes with conflict checks. Deletes and arbitrary runtime
operations are irreversible; completed plans and durable receipts never authorize
an automatic rerun or rollback. No accepted-source predicate or per-fault schema
is required for these general actions. API schema validation still applies.
`RemoveServicePort` is a convenience selector over the general patch implementation;
`EnsureDatabaseUser` uses the general container-command transport for mutation.

## Container commands, HTTP, and one-off scripts

`Command{Pod, Container, Args, Stdin}` carries an argument vector and optional input
stream. This covers arbitrary database administration, reloads, file edits and tar
input for copy. Output and stderr are discarded; commands express observations as
exit codes. Resolve credentials inside the container, including Secret-backed env.

`EnsureExec{Observe, Ensure, Expect}` observes first, runs ensure only when the
exit code differs, and observes again. `Expect` defaults to zero. Diagnosis uses
`ReadView.Observe(ExternalOperation{Exec: &command})`. Choose an observation that
actually distinguishes the state the repair changes; a successful process alone
is insufficient evidence of repair.

`EnsureHTTP` has the same observe/ensure/expect contract using `HTTPRequest`.
Requests run curl inside a selected Pod/container. Nonzero curl exit codes include
HTTP errors. Bodies and stderr are discarded, redirects are not followed, and
credentials come from `Bearer` or arbitrary `HeaderEnv` environment references.
For protocols or authentication outside this helper, use a container command.

`RunJob{Object: ApplyObject, Completion: WaitFor}` creates a named Job or Pod and
waits for measured completion. Its manifest supplies the image, script, resources,
service account, and credential references. A started script may have irreversible
external effects even if completion cannot be observed.

## Exact recorded transitions

The controller records `ExternalEffect` with exact observe/ensure operation
identities and measured before/after/command exit codes. Replay matches the whole
pair and its observed precondition. It never substitutes Fixed replies or claims
arbitrary command semantics from an executable name. HTTP follows the same rule.
`KubernetesEffect` carries measured before/after images for an exact raw API
request; unavailable images remain unknown. `JobCompletion` carries the actual
status for the exact one-off manifest. Action reports retain these measured
effects and recording errors even if capture I/O fails.

Use those measured records when constructing a new replay case. Missing transition
evidence is an evidence gap; it is no longer an SDK inability to execute a command.
Replay reports disclose `simulated:recorded-external-transition` and
`simulated:recorded-job-completion`. These model recorded transitions, and supply
no live health or credential-validation credit. Independently verified health
remains authoritative. Do not replay a write while refreshing read recordings.

The raw menu uses protobuf action names; its `EnsureHttp` entry corresponds to
Go's `program.EnsureHTTP`. JSON byte parameters such as `patch_json`, `object_json`
and API `body` accept their raw JSON/document content through the menu adapter.
