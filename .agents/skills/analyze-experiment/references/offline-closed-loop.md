# Offline closed-loop reports

`sdo/operator/cmd/sdo-replay` builds the accepted program/detector registrations through the production builder, then runs `runtime/replay` on explicit CorpusCase fixtures and accepted source manifests. Reports are JSONL outside the repository. They are synthetic offline evidence, never independent live outcomes, program trust credits, broker ACKs or benchmark receipts.

Each row has `program_id`, `case_id`, `status`, `expected_match`, `healthy`, `faulted`, `post`, `detectors` by phase, bound object identities, action statuses/value provenance, `effects`, `false_positives`, `errors`, and `duration_ms`. Detector-only matrix rows use `program_id: detector:<id>` and status `detected` or `negative` when correct; program rows use `replayed` or `negative`. `failed` and `refused` never prove repair. A passing detector-only row proves detection, not mitigation.

Detector observations carry `class` from existing `Spec().Class`, validated against the memory manifest by builder registration contracts. Health detectors fail for firing on Healthy (`health_false_positives`) or silence on Open with visible impact (`health_false_negatives`). Optional corpus label `health_impact: true|false` defaults to true; false permits health silence for latent faults or healthy-only cases, while Healthy must always be silent. Health firing on another family's Open is `overlap`, which passes. Health detector `expected_match` follows health impact, while incident detector and Diagnose expectations follow family labels. Incident detectors and Diagnose still fail for firing/matching on Healthy or other families (`incident_false_positives`) and for silence/NoMatch on their own family (`incident_false_negatives`), even when health impact is false. Unknown and observation errors stay failures without counting as false negatives.

These four category fields and `overlap` are trigger lists, omitted when empty. Legacy `false_positives` combines the two FP lists and excludes overlap. Stderr has five separate tables and deduplicates (category, case, trigger) per repetition; linked detector categories can repeat on program and detector JSONL rows, so raw list totals need the same deduplication. Overlap does not make the CLI fail. `detected` means active on Open; `negative` allows silent health detectors only when impact is false.

`real:program.<Action>:action:<index>` means the production PlanExecutor applied the action through the fake Kubernetes client. `simulated:apiserver-metadata`, `simulated:rollout-status:<kind>/<name>`, `simulated:pod-recreation:<old-name>` and `simulated:pod-ready:<new-name>` disclose assumptions absent from client-go. Readiness/rollout completion requires a template matching accepted source; `simulated:pod-readiness-unresolved` and `simulated:rollout-unresolved` disclose unavailable models. Source/parameter writes must restore the accepted target value even if a weak diagnosis clears. Ready is simulated, not evidence of successful scheduling, application startup, network connectivity or correct credentials. Legacy Exec without a recorded observation/effect pair remains unreplayable. General EnsureExec/EnsureHTTP use exact measured transitions through the production executor. Unsupported post-plan primitive observations remain Unknown and fail; fixed recordings never supply a post-plan answer. Source diffs are recomputed from the mutated state and explicit accepted manifests. Rollback needs explicit revision fixtures.

The checker compares final objects with production executor after-images, plus explicitly named controller reactions, and checks other fields/objects for unintended changes. Matrix mode evaluates every detector and Diagnose on every fixture; root-cause labels identify matching families and detector Playbooks associate detectors with those families. Missing bindings, observation errors and Unknown remain failures. The CLI requires a linked detector for every expected matching program.

Accepted source inputs use per-case `accepted.yaml` or `accepted.json`, or a shared `-source` file. Empty source inventories must be explicit. No Secret manifests belong in these inputs. Timing summaries measure replay after one compilation; distinguish repeated execution of one fixture from distinct fixtures and from a live experiment.

The existing SDK-only `sdktest.CheckMitigation` remains a compatibility helper for existing validation callers. Its legacy `skipped_unreplayable_effect` note is not a closed-loop result and proves no repair; the runtime checker never emits that status.

Detectors now replay actual logs, TCP/HTTP probes, source/baseline diffs, PromQL,
alerts and traffic through the same shared view as Diagnose. Phase snapshot
`readings` are combined with the selected phase's recording file; absent or failed
reads remain detector errors/Unknown even when agent code ignores accessor errors.
Frozen snapshot telemetry is reusable. Only actual Open/Healthy observations are
used for those phases. Post-plan observations are still unavailable unless the
checker can independently derive them; Fixed recordings never prove a repair.
Detectors can call `DetectionContext.DiffAgainstSource` before playbook selection.
After a plan, source comparisons and detector reads use the executed fake API;
only its 404 proves deletion. An omitted captured object remains unavailable.
Pre-execution recorded comparisons may establish phase-bound absence.

## Operator imports

`human` / `import` accepted broker commits carry `SDO-Origin: operator`. This is operator authorship, not responder learning or a captured incident. Each hand-written positive case has an `operator.json` object (`schema_version: 1`, `origin: "operator"`, nonempty `description`) beside the normal five corpus files, plus an explicit `accepted.json` source inventory. Captured provenance must be absent; accepted cases remain immutable. Programs run the production `runtime/replay.Check` during the independent validator build, alongside unchanged generalization, compilation, vet, tests and secret rules. The import grants no accepted outcome, origin learning credit, or live trust. `memory import prepare` returns a base-bound broker checkout; `memory import publish` uses the ordinary isolated Kubernetes validator. Do not present an offline replay success as a live mitigation.

## Adopted existing memory

Explicit `memory adopt-existing` / `install-existing --adopt-existing-memory` commits carry `SDO-Origin: adopted-existing` and `SDO-Validation: skipped`. Report these as an unvalidated accepted base, never as a successful import/replay. Only controller/program compilation is required initially. The exact unchanged inherited memory can be rebuilt without corpus acceptance; edits lose this exemption and follow normal broker validation. Inherited outcomes remain historical documents but supply no learning credit after adoption. Live executor refusals remain refusals. Healthy detector findings and excluded gallery detectors must be reported explicitly.

## Typed Service-port and MongoDB catalog repairs

`RemoveServicePort` receipts describe a conditional removal of exactly one Service port; final-port removal
requires explicit allowance. Replay applies the production patch and preserves unrelated fields.
`EnsureDatabaseUser` is an irreversible typed catalog operation, separate from arbitrary Exec. It restores
one user's existence/direct roles, preserves existing passwords and unrelated users, and reports unknown
on uncertain transport, changed Pod identity, or failed catalog read-back. Plans retain only env or target-namespace ConfigMap key/optional assignment references.
The executor resolves ConfigMap values privately into exec stdin; argv, receipts and corpus exclude them.
Direct Secret-key refs are deferred because installed RBAC allows metadata list/watch only. Replay seeds catalog state from successful open `DatabaseUsers` calls,
requires the recorded auth/shell configuration, and projects the typed mutation. Fixed replies, passwords,
authentication success and arbitrary commands are never simulated as repair proof. `real:program.EnsureDatabaseUser`
identifies the production executor path through this catalog simulator; it is not a live database receipt.


## General operations and residual SDK gaps

General object operations replay through the production executor and fake API, including
JSON/merge/strategic patches, list edits by unique match, arbitrary apply/delete, metadata,
cluster scopes and CRDs. `docs/sdk-action-inventory.json` is enforced by
`TestGeneralKubectlCoverageFloor`; adding an operation requires an executable replay case.

`ExternalEffect` records exact observe/ensure identities and measured exit-code transitions.
Replay requires the full pair and its before observation. `JobCompletion` supplies actual
status for the exact manifest, and `KubernetesEffect` supplies measured images for an exact
raw API request. No Fixed reply proves an effect. Reports disclose the assumptions with
`simulated:recorded-external-transition` / `simulated:recorded-job-completion`.
`ActionReport` retains `external_effect_json`, `kubernetes_effect_json`, `job_status_json`
and `recording_error` even when corpus capture I/O fails. Commands carry stdin but never
stdout/stderr or resolved credentials. HTTP credentials are environment references. The
controller discards TokenRequest response bodies without reading credential bytes.

The broker appends reflection's typed `sdk_gaps` to the existing accepted OutcomeRecord
(field 50), preserving the original incident evidence and original reflection-task commit.
`SdkGap` retains fault class, primitive, reason, exact `scratch_script`, and resources.
The definition moved from worker.proto to outcome.proto without changing its message name.
Historical runs may retain these only in ledger `reflection_decision` / `reflection_results`.

Run `uv run python scripts/list_sdk_gaps.py <outcomes-or-ledgers>...` offline. It prints
one deterministic JSONL row per incident/gap, with every source path, and deduplicates
outcome/ledger/retry copies. Include both outcome directories and broker ledgers to cover
older runs. `--fail-on-gap` exits 1 if any gap exists; malformed/unreadable input exits 2.
No cluster, benchmark verdict, or model is consulted. A gap is evidence of SDK demand,
not evidence that the incident was fixed or the proposed operation would be correct.
