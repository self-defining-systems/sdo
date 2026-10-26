# Recorded Kubernetes reads

Construct `client, err := program.Kubernetes(view)` once per program invocation.
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
subresources, with upstream signatures. Pod `GetLogs` accepts `PodLogOptions`.
`Dynamic().Resource(schema.GroupVersionResource{...}).Namespace(ns)` reads any
kind, including CRDs. `RESTMapper().RESTMapping` resolves API discovery once and
caches it for this invocation. No handwritten kind inventory is required.

Each request uses the existing cycle cache, primitive recorder, JIT read bridge
and phase replay. Keep observation streams finite. Missing recordings never
become an empty list or a healthy response. Secret endpoints and mutation
methods are unavailable to this read client; write intent remains an executor
plan. Exec effects require explicit undo rather than object restoration.
