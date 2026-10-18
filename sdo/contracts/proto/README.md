# SDO contract protos

Single source of truth for the structured contracts that cross SDO's seams:
controller↔worker tasks and results, worker registrations, reflection, outcomes,
firing telemetry, controller config, and traffic workloads. Agent-local Pydantic
views live in `sdo/agents/views/`; they do not define the cross-component wire.

## Layout

```
sdo/contracts/proto/sdodev/contracts/v1alpha1/   # versioned contract messages
```

The proto package is `sdodev.contracts.v1alpha1` (not `sdo.contracts...`) so the
generated Python root does not shadow this repo's real top-level `sdo` package;
`sdo/contracts/proto.py` puts `sdo/contracts/_gen` on `sys.path` and re-exports
the messages under a stable import path.

Generated code is written to:

- Go:     `sdo/operator/contracts/gen/`  (module `sdo.dev/operator/contracts`)
- Python: `sdo/contracts/_gen/` (includes a vendored classic-runtime
  `buf/validate/validate_pb2.py`; see below)

Generated files are committed so builds and tests do not require the toolchain at
compile time; CI regenerates and asserts no diff.

## Regenerate

```bash
scripts/gen_proto.sh     # buf lint + buf generate
scripts/check_proto.sh   # buf lint + buf breaking + assert committed codegen matches
```

Toolchain (installed via `go install`, so they land in `~/go/bin` — add it to `PATH`):

- `buf` — `go install github.com/bufbuild/buf/cmd/buf@latest`
- `protoc-gen-go` — `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`
- Python: the remote buf plugins `buf.build/protocolbuffers/python` and `.../pyi`,
  **pinned to `v33.4`** in `buf.gen.yaml`. That pin matches the `protobuf==6.33.4`
  runtime locked in `uv.lock`; the unpinned (latest) plugin emits gencode newer than
  that runtime, which then refuses to load it. Bump the two pins together with the
  protobuf runtime. The buf registry is reachable in this environment, so no local
  `protoc --python_out` fallback is needed; if it ever becomes unreachable, swap the
  two remote plugins for `local: protoc` with `--python_out`/`--pyi_out`.

Generated Go lives in its own module `sdo.dev/operator/contracts` (`sdo/operator/contracts/`,
go_package `.../gen/...`); consumers add a `require` + relative `replace` for it.

**Follow-up (not this refactor):** the `contracts` module needs `google.golang.org/protobuf`
v1.36.x (the gencode runtime), while `sdk`/`core`/`runtime` still pin v1.33.0. Consumers
therefore carry a `require` + relative `replace` on `contracts`. The clean endgame is to
converge all controller modules onto protobuf v1.36.x so the version skew (and eventually
the `replace` directives, once the module publishes) can be dropped. Deferred to avoid a
dependency-bump ripple mid-migration.

Relatedly, the relative `replace` directives are fragile because Go `replace` is **not
transitive**: a consumer that depends on `runtime` does not inherit `runtime`'s
`replace sdo.dev/operator/contracts => ../contracts`, so every main module in the graph
must repeat it. The synthesized detector workspace hit exactly this (it had to grow its own
`require` + `replace` for `contracts` in `sdo/operator/builder/workspace.py`, and both
Dockerfiles copy `sdo/operator/contracts`). A `go.work` workspace, or publishing the
`contracts` module, is the cleaner long-term fix than per-generated-`go.mod` replaces. Same
deferral.

## Rules

- These messages are serialized as **protojson**, never binary, on the wire.
- Semantic invariants (ordered timestamps, token arithmetic, unique IDs) are expressed
  with **protovalidate** (CEL), so validation is single-source too. The CEL is checked
  on the Go side with `buf.build/go/protovalidate` (see
  `sdo/operator/runtime/contract_conformance_test.go`). The one piece CEL cannot do —
  the `UsageMetrics` cache-read alias *normalization* (a mutation) — stays a thin
  hand step at the seam, noted in `messages.proto`.
- The Python gencode imports `buf.validate` for those field options. `protovalidate`
  on PyPI (v2+) ships only the newer `protobuf-py` runtime form, incompatible with the
  classic `google.protobuf` runtime this repo uses, so `scripts/gen_proto.sh` vendors
  the classic `buf/validate/validate_pb2.py` into `sdo/contracts/_gen/buf`
  (via `buf.gen.validate.yaml`). The Python agent edge validates its local views
  with Pydantic and parses protobuf with unknown-field rejection. Running the
  same CEL rules in Python remains a separate validation improvement.
- Do **not** add gRPC services here — SDO currently exchanges ProtoJSON through
  Job ConfigMaps and uses Git for proposals. A future transport can reuse the
  same task/result messages.
- `AgentRegistration` declares capabilities, protobuf type URLs, and a launch
  artifact. It does not specify the worker language or agent framework.

## Classic-protobuf-runtime bridge

The repo runs the classic `google.protobuf` runtime (`protobuf==6.33.4`), while the
current `protovalidate` tooling targets the newer `protobuf-py` runtime. Three choices
bridge that gap today while keeping a single classic runtime:

1. **CEL is checked on the Go side only.** `buf.build/go/protovalidate` validates the
   generated Go messages; `protovalidate` is deliberately *not* in `uv.lock` because v2+
   would drag in the incompatible `protobuf-py` runtime. A future
   classic-compatible Python validator can close this asymmetry.
2. **Vendored `buf/validate/validate_pb2.py`.** The proto field options force the Python
   gencode to `import buf.validate`; `gen_proto.sh` vendors the classic form into
   `sdo/contracts/_gen/buf`. If a future validator ships a classic `buf.validate`, drop the
   vendoring.
3. **Proto package renamed `sdo.contracts` → `sdodev.contracts`** so the generated Python
   root does not shadow the real `sdo` package (the facade puts `_gen` on `sys.path`). If
   the Python import strategy changes, the rename can be reconsidered. go_package is
   unchanged and protojson is field-name based, so the wire is unaffected either way.
