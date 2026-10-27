# Native acceptance and memory evidence

## Broker incident ledger

Source repairs change source identity (all non-`.sdo` inputs participate). Diff, reconcile and closure render accepted source directly; no rendered lock or image evidence exists.

For active Go runs, inspect the configured runtime `LedgerRoot` and the Go broker workspace root. The broker stages the deployer receipt before publishing source, then reconciles it to the attributed accepted commit after a crash. The receipt binds the candidate Git tree, strategy, image references, resource IDs, and observed ready revision; it is information and never gates a diff, reconcile or closure.

Ledger files under `.git/sdo-broker/` and the controller's runtime-state ConfigMap are canonical ProtoJSON of `BrokerLedger` and `ControllerState` (`controller_state.proto`): snake_case keys, empty and zero fields omitted (so a missing key means the zero value), RFC3339 timestamps, enums as names, and the closure as an `IncidentClosure`. The Go structs carry no JSON tags, so pre-cutover ledgers with legacy keys (for example `playbooks` or `path` in place of `playbook_ids`) are not readable by the current tools. `reflection_rejections` lists each broker rejection of a reflection proposal (`attempt`, typed `diagnostics` of `rule`/`path`/`line`/`message`/`hint`, `rejected_at`); Go re-dispatches reflection with the previous rejection's diagnostics up to twice, and `reflection_rejected` marks the terminal rejection after the bound (the closure completes with no reflection commit). Current native ledgers use `reflection_rejections`, `reflection_rejected`,
`reflection_processed`, and typed `reflection_decision` (decision, reason, logical
playbook IDs, error, SDK gaps and usage). `ReflectionDecision.playbook_ids` remains
wire field 3. Every closed incident goes through learning settlement; inspect the recorded reflection decision and any explicit skip reason independently of program counters. A failed learning
turn preserves the committed mitigation and records its error. The frozen reflection task's
`ReflectionRequest.depth` is `refine` (Go-decided, resumed responder session only, first attempt
only) when installed programs were applied, not dry-run or rolled back, and have bound post-apply
NoMatch tool evidence; every repair action is one of those applications; every matching binding was
applied and every confirmed root cause is covered by an applied binding; and there is no program
failure signal, drift event, exhausted loop or source repair. The prompt is then a few KB and the
expected decision is `no_change`. Empty or `full` (fresh fallback, retries, anything else) is the
complete authoring reflection. The closure's `request.state_changes` (absent in older ledgers or without a healthy baseline) is the controller's configuration diff against its last healthy baseline when the incident opened: `baseline_at`, `observed_at`, `changes` (`kind`, `name`, `change` of `added`/`removed`/`modified`, and field-level `before`/`after`, where ConfigMap data appears only as `sha256:` digests; current Secret fields are only `uid` and `resourceVersion`, with the object name on the change. Secret content drift is a resourceVersion change, never a data digest. Strict metadata-only list/watch has no full-object fallback; unsupported or forbidden metadata reads mark `Secret` unobserved. Secret type and key names are unavailable in PartialObjectMetadata and omitted. Older ledgers may contain Secret data digests), `omitted`, and `unobserved_kinds`; objects that existed unchanged at the baseline, such as SREGym's `failure-admin-*` decoys, never appear. The closure's `final_state_changes` (absent in ledgers before the verification-time state diff, and without a healthy baseline) is the same shape of diff, recomputed at verification time instead of dispatch time: a composite fault's later component can land a few seconds after dispatch while the incident is still open (K2's readiness-probe fault about 6s after its selector fault), so it can appear here without appearing in `request.state_changes`. Current derivation records these diffs for context without checking diagnosis references. Historical closures may also carry `observed_state_changes` to date cited changes against repairs; current closures omit it. `health_cleared_at` records the start of the final health-clear streak, without root-cause attribution. Program trust retains its separate bound application and NoMatch checks. The closure's `cleaned_helpers` (absent in older ledgers) lists the responder helper pods and Jobs (`sdo.dev/responder-helper=true`) the controller deleted when the responder completed. The closure's `incident_detector_states` (absent in older ledgers) holds the latest post-response evaluation of each non-health detector that raised a finding; it is learning evidence, not a closure gate. A closure with empty `final_detector_states` means health never cleared within the controller's verification window; the broker classifies it `failed` and strict receipt validation rejects it as a full mitigation. Reflection can still run for an independently verified partial repair, as described below. A missing proposal commit is valid under `recorded-actions`; current success classification uses successful repair actions, while verified clearing without a repair is `cleared_without_action`. Receipts do not re-check diagnosis or repair evidence. Require incident IDs and commit hashes to agree with the receipt and outcome.

`IncidentClosure.source_check_notes` and `OutcomeRecord.source_check_notes` list source or render checks that were unavailable when health verified the closure (accepted source would not render, the source inventory had not loaded). They are information only: the closure stays a success decided by health alone, and only an observed source difference contradicts it. Absent in older ledgers and outcomes.



### Historical Python broker fields

These fields describe retired Python ledgers, not current ProtoJSON:
`reflection_attempts` counts turns including backend failures;
`reflection_backend_error` is the last such failure;
`reflection_fresh_retry_attempts` counts fresh retries after validation rejection.
`reflection_session_mode` records `fresh` or `resume`; the first turn fixes it.
The rejected diff was `<sha256(incident_id)>.reflection-rejected.diff`.
`reflection_skipped_reason` explained deterministic reuse from verified exact or
learned-detector matches and trusted successful playbooks. Such a skip used zero
reflection attempts, empty usage, a `no_change` decision with the skip reason,
and no reflection commit, accepted detector paths or rollout. Verify those
historical claims against the incident's detector, action and outcome evidence.

## Operational memory layout (schema version 3)

Detectors are registered by per-detector manifests at `.sdo/diagnostics/detectors/<health|incidents>/<id>/manifest.yaml` (the directory name is the detector ID). The former single `.sdo/diagnostics/manifest.yaml` index exists only in schema version 1 repositories; Upkeep migrates it. Schema version 3 also stores playbook references (`possiblePlaybooks`, detector `Playbooks`) as logical IDs; version 2 repositories used `.sdo/playbooks/<id>/README.md` paths. Readers must enumerate detector manifests instead of parsing one index.

Program and drift integration: controller state and broker ledgers use generated ProtoJSON. State carries ownership adoption, ordinary batching and retained incident drift. There is no active investigator worker, task/result protocol or verdict ConfigMap. Synthetic IDs are `drift.<program>` and `drift.unexplained`. Empty repeated fields may be omitted. Removed config tags 59–61 and 67–68 and state tags 32–33 are reserved; use coordinated worker/controller images after draining. Schema version 3 classifies program Go sources, shared libraries and corpus through `memory/layout`; policy/replay tests are centralized under `memory/validate`.

Go dispatch is the reflection replay unit after the mainline cutover. Each checkpoint replays one worker result through the shared validator and real broker; broker rejection/retry history remains in the Go ledger. `PlanApplication` retains concrete applied actions, verification outcomes and timing through ProtoJSON restarts; `ProgramToolCall` retains its summary.

### Controller lifecycle phases

`ControllerState` uses generated `DispatchState` and `ClosureState` enums:
`DISPATCH_STATE_PENDING`,
`CLOSURE_STATE_PENDING`, `CLOSURE_STATE_COMMITTED`, `CLOSURE_STATE_FAILED`.
Absent fields mean `*_UNSPECIFIED`; old lowercase strings and undefined numbers
are rejected. `runtime/internal/lifecycleio` persists generated messages.
`BrokerLedger.reflection_phase` drives dispatch/result inspection/review/publication/
reset/done; `reflection_results` retains completed decisions after rejection.
Frozen requests live under the ledger's `reflection-tasks/` directory.
`ControllerState.failed_closures` is an outbox, archived before the next state CAS
as content-addressed `sdo-failed-closure-*` ConfigMaps (`failed-closure.json`, label
`sdo.dev/closure-archive=failed`). Exhausted closures retain evidence and allow
later incidents; archives are runtime evidence, not operational memory.


For incident restart evidence, read [native incident recovery](native-incident-recovery.md).

### Accepted reachability baseline evidence

The optional health-owned `.sdo/diagnostics/detectors/health/service-reachability/baseline.json` records fresh failed dial samples captured by the trusted, opt-in healthy-baseline gate while all other health detectors and the authored objective independently passed. It is broker-published as `health_judge` in `initial`, followed by detector-bundle adoption. Read version, authorization/source/objective bindings, capture time, exact targets, configuration hashes and measured samples. A missing historical success alone never authorizes an exclusion. A changed source/objective/Service name or target/backend/config/policy invalidates the record for that target; a resulting finding reports `baseline_status`. A restart reuses the accepted record, including an empty record, and restored incidents cannot capture their failures as baseline exclusions. Candidate failures in saved logs are not accepted exclusions without this measured healthy-baseline evidence.

For partial repair, reflection publication and retirement evidence, read
[native reflection](native-reflection.md).

## Retired playbooks

Playbook README front matter may set `status: retired` with a nonempty string
`retired_reason`; omitted status or `status: active` is active. The stored README,
program source and immutable corpus remain evidence. Retirement removes the
playbook from worker snapshots, detector hints, generated program registration,
coverage and mitigation menus. A README change reloads the controller registry.
Do not count a retired README as reusable learned coverage; inspect its reason
and independently verified outcomes when assessing failed or undone mitigations.


Reachability configuration identity ignores Service UID/resourceVersion, allocated ClusterIP/ClusterIPs, IP-family defaults, NodePort and health-check NodePort, plus Pod/endpoint incarnation addresses. It retains namespace/Service name and ports (name, number, protocol, targetPort), selector, headless routing, other Service policy fields, deduplicated selected container configuration and labels, referenced ConfigMap contents and selecting network-policy specs. NodePort values are ignored even when explicit: TCP observations use Service DNS and Service port; source-input authorization separately binds authored changes. ServiceUID remains capture provenance only. A recreated equivalent Service keeps its never-served observations without rewriting the immutable reference; a later successful dial restores the ordinary three-failure/20-second gate. Pre-change opaque configuration hashes cannot be safely migrated from the reference alone and remain fail-closed; use a fresh verified install boundary for those records.


For `code_path.name: shell`, reflection activation uses the existing runner instead
of a controller rebuild. Read `closure_phase_timestamps.shell_runner_activated`
against `reflection_commit`; no `rollout_expectation` is required. The runner's
read-only results report includes its accepted commit, activation time, and each
script's exit code, duration (Go duration nanoseconds), stdout/stderr tails, time,
error and fired streak. Two nonzero runs fire; errors reset the streak and block
health. Controller closure still owns recovery and its stability evidence. Keep
activation latency separate from script execution and responder runtime.
## Ordered response stages

`ControllerConfig.code_path.overrides.response_stages` defaults to `deterministic,agent`; `agent`
alone is the baseline. `IncidentRequest.prior_stages` and
`OutcomeRecord.prior_stages` retain typed controller-owned stage reports.
Each mitigation identifies the accepted playbook revision, bindings, plan,
`apply_receipt`, post-apply Diagnose, and cleared/remaining finding fingerprints.
Results distinguish `applied_cleared`, `applied_still_matching`, `rollout_failed`,
`rollout_pending_at_handoff`, `apply_error`, and `skipped` with `skip_reason`.
Stage-level fingerprints describe work remaining at each handoff. These scoped
observations never replace independent controller closure/classification.

The deterministic stage requires a fresh Match, exactly one mitigation entry
point, and all parameters bound from covered findings. It has no trust gate and
never automatically undoes a completed apply. Its own rollout wait is capped at
30 seconds. Interrupted apply intent survives in the incident tool checkpoint;
a retry reports uncertainty instead of repeating a potentially landed mutation.

`resolution_path: deterministic` means the incident needed no responder worker;
usage is zero model work, while recorded plan actions remain repair evidence.
These incidents still receive reflection using a fresh session and the same
stage reports; a missing session from a failed responder does not enable that
fallback. Attribute learning only from the accepted reflection and independently
verified cause evidence. The responder's inline view summarizes stages; complete
receipts remain in its private incident facts and committed outcome.

### Receipts after responder retirement

The controller may retire a responder Job after an early report. Post-reflection receipt collection uses the broker ledger's frozen incident request, correlated result, durable dispatch/completion timestamps, independently verified closure, and accepted acknowledgment. A live responder Job is not a receipt prerequisite. An empty `responder_job_evidence` means no Kubernetes success claim is made; the Go checker requires valid ordered lifecycle timestamps and a completed correlated result instead. Historical receipts with Job evidence retain their original validation.
