# Recorded Kubernetes reads

Construct `client, err := program.Kubernetes(view)` once per program invocation.
Pass `bindings` as the optional second argument for source-grounded roles;
`StaleBindingError` identifies a role removed from accepted source. Omit that
argument for intentionally source-absent or derived objects.
A detector uses `program.Kubernetes(program.ReadFrom(ctx))`. Handle construction
and read failures as errors/Unknown, including missing offline observations.

Use client-go conventions and Kubernetes types:

```go
client, err := program.Kubernetes(view)
if err != nil { return program.Verdict{}, err }
ref := bindings.Objects["workload"]
items, err := client.AppsV1().Deployments(ref.Namespace).List(ctx, metav1.ListOptions{
    LabelSelector: "app=" + ref.Name,
    Limit: 100,
})
if err != nil { return program.Verdict{}, err }
```

Use `ListOptions.Continue` from the returned list to fetch subsequent pages.
Pass field selectors and resource-version options directly, according to the API
server's consistency semantics. Derive labels and names from bound roles and
observed relations. Read concrete typed fields so compilation checks API changes.

All pinned built-in groups expose their Get/List/Watch methods and read
subresources, with upstream signatures. Pod `GetLogs` accepts `PodLogOptions`; `Metrics()` returns upstream metrics clients and quantities.
`Dynamic().Resource(schema.GroupVersionResource{...}).Namespace(ns)` reads any
kind, including CRDs. `RESTMapper().RESTMapping` resolves API discovery once and
caches it for this invocation. No handwritten kind inventory is required.

Each request uses the existing cycle cache, primitive recorder, JIT read bridge
and phase replay. Keep observation streams finite. Missing recordings never
become an empty list or a healthy response. Secret endpoints and mutation
methods are unavailable to this read client; write intent remains an executor
plan. Exec effects require explicit undo rather than object restoration.


## Execution capability

`program.KubernetesForExecution(view, executor)` exposes the full pinned upstream
clientset and dynamic client through an admitted `KubernetesExecutor`. It supports
Create/Update/Patch/Delete, scale subresources and server-side apply; apply defaults
to field manager `sdo`. Results come from the actual API. The read view alone does
not grant this capability: current `Mitigate` methods still produce durable Plans.
The Go host's `PlanExecutor.NewKubernetesExecution` collects the existing effect
receipts and measured images; `Result` returns a detached application receipt.

Named API configuration patches with complete images support conditional undo.
An intervening configuration change refuses undo; conflict retries reread current
versions. Deletes, connection upgrades and opaque external effects remain disclosed
as irreversible. Proxy receipts add a per-request object-image reference and an
explicit undo-unavailable reason for exec and Secret operations. Secret data is
never read into capture storage. Use existing recorded Command actions for exec.
