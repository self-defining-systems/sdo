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

## Read errors have a class
A `ReadView` error says which of four things happened. Test it with `errors.Is`; an error with no class
is `sdk.ErrUnavailable`.

| class | sentinel | meaning | what a program may do |
|---|---|---|---|
| observed | none (a value) | SDO saw the state, including "not there" | reason on it |
| refused | `sdk.ErrRefused` | the target answered and said no: a database login or catalog read the database rejected, a credential reference with no usable value in the container | reason on it, for example try the next credential |
| unavailable | `sdk.ErrUnavailable` | SDO could not observe: no client, kind not captured, timeout, transport, no accepted source, a read a recording does not hold | only "I do not know" |
| invalid | `sdk.ErrInvalidRead` | the program asked something illegal: a Secret read, a ref outside the namespace, an ambiguous database target, the read budget | a program bug; fix the program |

`sdk.ReadErrorClass(err)` names the class.

Replay agrees with live. A recording stores each failed read's `error_class` next to its text, a replay
rebuilds an error of the same class, and a recording written before classes existed reads as unavailable. A
database catalog read is recorded by target and credential reference (never a value), so a fallback that
tries one credential and then another replays both reads.
