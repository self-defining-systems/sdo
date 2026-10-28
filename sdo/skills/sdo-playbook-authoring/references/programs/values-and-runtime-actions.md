## Values and runtime actions

`SetField.Value` is an opaque `program.Value`: use `FromSource(ref, path)` for the incident-bound accepted
manifest, `FromObserved(ref, path)` for this invocation's ReadView snapshot, or `FromParam(name)` for a
declared scalar parameter. Raw Go literals do not compile, and zero values fail validation. The executor
resolves at apply time and reports the source commit/path, observed resourceVersion/path, or parameter name.
Secret-typed references fail before lookup; no Secret-derived resolved value belongs in reports or receipts.
Observed values remain bound to their program invocation when several plans share one apply batch.

Use `bindings.Objects[name]` for resource identities and `bindings.Values[name]` for scalar inputs.
`Bindings.Equal` uses the shared object citation parser: known kind aliases and omitted default API versions compare equally; names, namespaces, explicit versions and scalar values must agree.
Declare value parameters in `ProgramSpec.Params` with `ValueKind` (`StringValue`, `IntValue`, `QuantityValue`,
`DurationValue`, or `BoolValue`). Required scalar parameters need a `Default` so the live finding path
can bind them; read incident-specific values through ReadView. Diagnose and Mitigate must declare identical
parameters. Keep object `Kind` empty for a value parameter.
`ValidateBindings` rejects wrong types, unknown names, duplicate declarations, and mixed object/value kinds.
Use `bindings.Parameter(spec, name)` to retrieve a validated binding or its default. Quantities and durations
are strings in Kubernetes quantity and Go duration syntax; booleans and integers retain their scalar types.
Named constants such as a scale amount are defaulted parameters. An emergency literal is a declared
parameter the responder supplies and justifies; a permanent intended-state change requires a source repair.

`DeletePod{Ref}` deletes one named Pod with UID/version preconditions and verifies absence or a replacement
UID; a graceful delete leaves the Pod terminating, which counts as deleted and is reported `observed: terminating` (the Pod may still run until it stops). It never recreates a Pod during undo. `ScaleWorkload{Ref, Replicas}` accepts a Deployment, StatefulSet or
old-revision ReplicaSet of a bound Deployment, resolves a nonnegative integer replica count, and directly verifies `spec.replicas`. Partial failures undo
reversible fields from captured pre-images; irreversible effects remain disclosed.

`Exec{Pod, Container, Command: []program.Value{...}}` derives every command argument through values. The
controller identity performs the exec; a plan creates no grant.
Callers supply that controller transport through `RuntimeOptions.PlanExecConfig`; nil reports refused. Exec
can reach mounted Secret files, so stdout and stderr are never requested, returned, or stored. Only the exit
code is reported. Refused or uncertain execution earns no write or health-verification credit, and a durable
receipt never authorizes command replay after restart.

