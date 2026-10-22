# Boundary type inventory

Baseline: `f0927949e` (R2 slice 1). Paths below are relative to the repository;
proto names are under `sdo/contracts/proto/sdodev/contracts/v1alpha1/`.
**G** = proto-backed and generated at the seam; **H** = proto-backed but
hand-parsed/built at one or more consumers; **N** = no shared protobuf schema.
A generated envelope does not make a string inside it a typed reference.

| Value crossing a boundary | Definition / boundary | Hand parsing or construction (production paths) | State |
| --- | --- | --- | --- |
| Worker registration, capability/type URLs, task/result, proposal checkout | `worker.proto`: `AgentRegistration`, `AgentTask`, `AgentResult`, `ProposalWorkspace`; Python ↔ Go | `worker_registry/{registry,kubernetes}.go`; `agents/worker.py`; adapter `driver.py` rewrites registration launch dictionaries | H |
| Source deployment, image builds, deployed resource IDs, architecture, judge requests/results | `lifecycle.proto`; LLM → Python → Go | agent-local lifecycle views; `operator/deployment/{backend,raw}.go` split resource IDs on `/`; adapter `native_operator.py:launch_host`, `driver.py:launch_native_lifecycle` read dict summaries | H |
| Controller install/inspect receipt, runtime artifact export result, incident-evidence wrapper | Go `operatorcli/{install_existing,inspect_existing,runtime_artifacts,incident_evidence}.go`; Go → adapter `native_operator.py` | JSON structs → `_run` dicts → `NativeInstallReceipt`/`NativeExistingReceipt`; `collect_native_receipt` hand-extracts ledger, pod and Job fields | N |
| Controller configuration | `controller_config.proto`: `ControllerConfig`; adapter → Go CLI | adapter `runtime.py:RuntimeConfig`, `native_operator.py:NativeInstallConfig.common_args`, `driver.py` hand-build flags | H |
| Kubernetes object refs in findings, bindings, actions, causes | `common.proto`: `ObjectRef`; LLM ↔ Python ↔ Go | `sdk/detector.go:ObjectRef` duplicate value struct; runtime contract projections; Python `agents/responder/tools_cli.py:_binding` splits `/` and supplies API versions; prompt formatters in `codex.py`, `reflection_brief.py`, `reflection_outcomes.py` | H |
| State-change evidence source (including namespaced and compound refs) | `messages.proto:RootCauseEvidence.source` string and CEL single-object regex | `outcome_derivation/diagnosis.go:parseKey`, `canonicalKind`, `evidenceKey`; Python `agents/responder/result_output.py` validates regex and `codex.py` retries; prompt examples hand-format sources | H |
| State-change observations and field deltas | `messages.proto`: `StateChanges`, `ObservedStateChange`; Go → agent/outcome | runtime state-change projections; `outcome_derivation/diagnosis.go` builds keys from kind/name; controller observations omit namespace | G |
| Incident request/result, status, cancellation, repair policy, response budget | `messages.proto`: `IncidentRequest`, `IncidentResult`; LLM ↔ Python ↔ Go | model-facing JSON schemas generated from the message descriptors by `sdo/contracts/model_schema.py` (omissions declared at the call site), wrapped with the reflection schema by the session wrapper in `agents/responder/session_turn.py`; agent-local output views; adapter `driver.py` fallback submission extraction | H |
| Root causes, verification, repair-action receipts | `messages.proto`: `ConfirmedRootCause`, `VerificationEvidence`, `RepairActionReceipt`; LLM → Go | generated worker decoding; `protocol/receipt.py:_RepairActionReceipt` duplicate model for retired receipt; `protocol/native_receipt.py` traverses raw result dictionaries | H |
| Outcome classification, diagnosis verdict/checks, repair attribution | `outcome.proto`: `OutcomeRecord`, `DiagnosisVerification`, `OutcomeRepairAttribution`; Go → Python | `outcome_derivation/{derive,diagnosis,drift}.go` derives generated records; `protocol/{receipt,native_receipt}.py` reinterprets classification and resolution strings | H |
| Incident IDs and full Git commit IDs | string fields in `messages.proto`, `outcome.proto`, `worker.proto`, `controller_state.proto` | runtime/broker correlate identities; `protocol/native_receipt.py` repeats identity and commit checks (`_COMMIT` regex); adapter `persistent.py:_known_incident_ids` collects strings | H |
| Logical playbook IDs, script names, detector IDs and artifact IDs | `messages.proto`, `memory.proto`, `program.proto`; LLM ↔ Go | `memory/layout/{paths,tree}.go`, `memory/validate/{load,semantic}.go` split logical IDs or physical paths; Python agent output schemas and `sdo/contracts/proto.py` descriptor-derived ID rules | H |
| Operational memory snapshots and deprecated artifact proposals | `messages.proto:OperationalMemorySnapshot`, `OperationalArtifactProposal`; Go ↔ agent | generated messages; Go memory materializes files; deprecated proposals rejected | G |
| Program tool requests/responses (candidate/handle, source, run, apply, capture) | Go runtime tool structs; Python `agents/responder/{tools,tools_cli}.py`; CLI ↔ tool server | JSON dicts, action/command switches; `tools_cli.py` builds bindings from ref text | N |
| Program specs, bindings, verdicts, evidence, plans/applications, preimages, tool-call records | `program.proto`; worker ↔ Go / durable outcome | runtime SDK ↔ proto projections; `protocol/native_receipt.py` checks fast-path plan verification via nested dicts and enum text | H |
| Usage and timing metrics | `messages.proto:UsageMetrics`, `TimingMetrics`; worker → Go → benchmark | `sdo/contracts/proto.py` normalizes cache-read alias; adapter `persistent.py:_timestamp`, `_resolution_timings`; `driver.py` copies timing summaries | H |
| Closure, acknowledgment receipt, failure, reflection decision, rollout expectation/receipt | `messages.proto:IncidentClosure`; `controller_state.proto`: `ClosureReceipt`, `ClosureFailure`, `ReflectionDecision`, `ControllerRolloutExpectation`, `ControllerRolloutReceipt`, `BrokerLedger` | `protocol/native_receipt.py` hand-validates all nested shapes, identity, clear states, reflection and rollout; `persistent.py:_closeout_settled`, `_raise_if_closure_failed`, `_collect_settled_receipt` repeat decisions | H |
| Controller-state fields read by adapter | `controller_state.proto:ControllerState`; Go → adapter | `persistent.py` reads `last_acknowledged_incident_id`, `pending_closure`, `incident_request`, `closure_state`, `closure_failure`, detector review fields; `driver.py` reads raw ConfigMap JSON; baseline/fault-gate resource and log parsing is a separate row | H |
| Controller evaluation and maintenance log facts | No shared message for legacy iteration/maintenance log envelopes; Go → adapter | `fault_gate.py:controller_active_findings` interprets `controller_iteration`, `findings`, `rule_id`, `status`; `persistent.py:_log_records`, `wait_for_maintenance_ack`, `_relaunched_after` interpret log dictionaries | N |
| Detector timeline, firing records, lifecycle events, late-findings reports/receipts | `messages.proto:DetectorTimelineEntry`; `telemetry.proto`; `late_findings.proto`; Go → worker/adapter | adapter `persistent.py:_log_records`, `_relaunched_after` parse legacy log dictionaries; `driver.py` copies timeline fields into receipt summaries | H |
| Strict production receipt v2 and per-incident receipt rejection | `protocol/native_receipt.py:native_production_receipt`; `persistent.py:ReceiptRejectedError`; adapter ↔ runner | receipt dict literal and `_object`, `_items`, `_nonempty`, `_timestamp`, `_commit`; `driver.py:persist_production_receipt`, `protocol/receipt.py:receipt_resolution` and validator dispatch | N |
| Retired production receipt v1 | `protocol/receipt.py:validate_production_receipt`; saved evidence → analysis/runner | dict traversal plus `_ControllerRolloutRecord`, `_RepairActionReceipt`; duplicated classification/attribution rules | N |
| Persistent adapter state, pending/deferred receipts, controller record | `adapter/persistent.py`: `PersistentState`, `PendingIncident`, `DeferredReceipt`, `ControllerRecord`; stage ↔ stage | Pydantic JSON persistence and nested untyped receipt/config dicts | N |
| Incident resolution/rejection, reflection drain and stage result summaries | `adapter/persistent.py:run_persistent_stage`, `drain_pending_incident`, `persist_resolution`; adapter → runner/analysis | dict literals, string dispositions, drain/closure failure exception handling; `driver.py` fallback and telemetry dictionaries | N |
| SREGym status, ready/terminal stage and app/problem metadata | `protocol/schema.py`, `conductor.py`; external HTTP → adapter | `_http.py`, `conductor.py:get_app_info`, `get_problem`, `wait_for_stage`; adapter `submission.py:_wait_for_stage`, `driver.py` extract dict keys | N |
| Diagnosis/mitigation phase, solution POST, acknowledgment, grading response | `adapter/submission.py`, `submission_relay.py`; worker → adapter ↔ SREGym | `_LATER_STAGES`, `submit_solution` guard; relay HTTP handler validates raw dict; `protocol/mcp_client.py` filters oracle dictionaries | N |
| MCP benchmark result and oracle wrapper | `protocol/benchmark.py:BenchmarkResult`, `Oracle`; SREGym → agent | XML-ish tag regexes, `parse_benchmark_result`, JSON oracle decoding; `mcp_client.py` stage/result dictionary selection | N |
| Fault injection/reset/cleanup and benchmark timing rows | `protocol/conductor.py`, adapter `fault_gate.py`, fastloop/runner transport | HTTP payload dictionaries, stage strings and error checks; no shared first-party proto | N |

## Reproduce the inventory

Use `rg -n 'split\(|partition\(|re\.|json\.|dict\[|ParseDict|MessageToDict'
benchmarks/sregym/{adapter,protocol}` for the Python seams, and
`rg -n 'strings\.(Split|Cut)|Unmarshal|Parse.*Ref|parseKey' sdo/operator
--glob '*.go' --glob '!*.pb.go'` for Go. Search hits for filesystem paths,
image registries and URLs are separate grammars, not object refs. Generated
files and tests are excluded from the ownership assessment above.

Highest-risk follow-ups: canonical evidence refs (A), Go-owned native receipt
validation with a typed incident failure (E), then submission phase decisions
and drain interpretation. R1 owns `controller_state.proto` and runtime
lifecycle code; R2 must consume its generated types rather than add a second
controller state representation. No operational behavior changes in this slice.

## Implemented receipt seam (R2 slice 3)

`benchmark_receipt.proto` now defines the v2 receipt core, Job evidence,
`NativeReceiptResult`, `ReceiptFailure`, `ReceiptDrainResult` and
`RejectedProductionReceipt`. Receipt correlation/classification/reflection/
rollout/fast-path decisions moved out of Python into the benchmark-only Go
module `protocol/native_receipt_go`. These rows are now **G** at the decision
boundary; timing/export annotations and raw diagnostic evidence remain opaque
transport metadata. Python forwards the raw ledger to the generated Go decoder
and decodes the generated result. Retired v1 receipt validation is unchanged.

The adapter drain returns a generated failure rather than raising
`ReceiptRejectedError`; persistent and fastloop callers consume that result.
The narrow fastloop caller edits follow the changed adapter API and contain no
new lifecycle decisions. Validity analysis decodes typed rejection artifacts to
keep checker unavailability separate from rejected agent evidence. Closure/drain interpretation,
controller-state dictionary readers and the other **N** rows remain follow-ups.

## Implemented submission seam (R2 slice 4)

`benchmark_submission.proto` now owns submission phases, terminal-stage names,
status/acknowledgment facts, the solution body and the Go phase decision. Python
forwards external facts to `protocol/submission_go` and consumes generated
`SubmissionGuardDecision`; its duplicate stage/acknowledgment decision logic is
deleted. The ready/terminal constants in `protocol/schema.py` come from generated
descriptors. These decision rows are **G**. Upstream reply metadata remains opaque
transport data, and the upstream status-read/POST pair is not atomic.

## Implemented controller observation seam (R2 follow-up)

Persistent-adapter incident/drain interpretation now uses the generated
`ControllerObservationRequest`/`ControllerObservation` CLI contract. Go owns
known/new incident correlation, verified health, acknowledgments, review ordering,
permanent closure failure and the open-incident summary. It consumes R1's current
generated enums, fixing the adapter's retired `"failed"` string comparison.
Undefined enums/unknown fields are typed snapshot failures; publication validation
and lifecycle transitions stay with R1. No controller-state/runtime files change.
The remaining driver snapshot reader is **H**; baseline/fault-gate log and
resource parsing and timing annotations remain separate seams; legacy receipt v1 and other opaque external data remain.
