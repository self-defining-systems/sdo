# Native acceptance and memory evidence

## Broker incident ledger

For bound incident workspaces and publication-wait evidence, read
[ordered response stages](response-stages.md).

A responder's source repair is the application source it committed in its checkout (published after the closure is acknowledged) plus any `.sdo/deployment/patches/NNNN-*.patch` it added; a deployer's source change is a patch or deployment file; effective deployment inputs (values, manifests, build, overlays, patches) participate in the health `SourceInputs` and the effective tree ID is the source identity. `patch.uncaptured` (app edits no patch reproduces) rejects a publish and is sent back to the agent as correction text where a turn remains. The closure only records `source_repair_pending` (ledger `source_repair: pending`); the queued dispatch after the acknowledgment deploys and verifies it with the trusted Deployer before the broker accepts it, under a maintenance pause, deferring (not failing) while an incident is open. The ledger's `closure_phase_timestamps` then has `source_repair_deployed` (accepted with new deployment evidence, so image pins follow) or `source_repair_held` (rollout, build, health or broker-rule refusal; nothing accepted, the outcome is unaffected) or `source_repair_uncaptured` (uncommitted app files discarded). The result is appended to the outcome as `source_repair {status, commit, detail, uncaptured_paths}` and `proposal_commit` is the accepted source commit. Runs before this change have `source_repair_uncaptured` set at closure, patch-only repairs deployed in the closure, and no `source_repair` on the outcome. A repair of deployment inputs on an install with no recorded SDO deployment, or with shell detectors or frozen memory (no repairer), is published without a deployment: the ledger has `source_repair_published_undeployed` (and a `source_repair_published_undeployed` log line), never `source_repair_deployed`, and image pins stay at the last deployment. Diff, reconcile and closure render accepted source directly; no rendered lock or image evidence exists.

For active Go runs, inspect the configured runtime `LedgerRoot` and the Go broker workspace root. The broker stages the deployer receipt before publishing source, then reconciles it to the attributed accepted commit after a crash. The receipt binds the candidate Git tree, strategy, image references, resource IDs, and observed ready revision; it is information and never gates a diff, reconcile or closure.

Ledger files under `.git/sdo-broker/` and the controller's runtime-state ConfigMap are canonical ProtoJSON of `BrokerLedger` and `ControllerState` (`controller_state.proto`): snake_case keys, empty and zero fields omitted (so a missing key means the zero value), RFC3339 timestamps, enums as names, and the closure as an `IncidentClosure`. The Go structs carry no JSON tags, so pre-cutover ledgers with legacy keys (for example `playbooks` or `path` in place of `playbook_ids`) are not readable by the current tools. `reflection_rejections` lists each broker rejection of a reflection proposal (`attempt`, typed `diagnostics` of `rule`/`path`/`line`/`message`/`hint`, `rejected_at`); Go re-dispatches reflection with the previous rejection's diagnostics under the bounded correction policy, and `reflection_rejected` marks the terminal rejection after the bound (the closure completes with no reflection commit). Current native ledgers use `reflection_rejections`, `reflection_rejected`,
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
complete authoring reflection. The closure's `request.state_changes` (absent in older ledgers or without a healthy baseline) is the controller's configuration diff against its last healthy baseline when the incident opened: `baseline_at`, `observed_at`, `changes` (`kind`, `name`, `change` of `added`/`removed`/`modified`, and field-level `before`/`after`, where ConfigMap data appears only as `sha256:` digests; current Secret fields are only `uid` and `resourceVersion`, with the object name on the change. Secret content drift is a resourceVersion change, never a data digest. Strict metadata-only list/watch has no full-object fallback; unsupported or forbidden metadata reads mark `Secret` unobserved. Secret type and key names are unavailable in PartialObjectMetadata and omitted. Older ledgers may contain Secret data digests), `omitted`, and `unobserved_kinds`; a change of kind `StoreState` (name `<component>/<pod>`, newer ledgers only) is a data store's value-free items (`setting maxclients`, `role role`, ...) that differ from the store-state readings promoted after health settled (`DefaultStateSettle`, in every mode; a refused read is never promoted; the readings are in-memory, so a restarted controller shows none until health re-settles); objects that existed unchanged at the baseline, such as SREGym's `failure-admin-*` decoys, never appear. The closure's `final_state_changes` (absent in ledgers before the verification-time state diff, and without a healthy baseline) is the same shape of diff, recomputed at verification time instead of dispatch time: a composite fault's later component can land a few seconds after dispatch while the incident is still open (K2's readiness-probe fault about 6s after its selector fault), so it can appear here without appearing in `request.state_changes`. Current derivation records these diffs for context without checking diagnosis references. Historical closures may also carry `observed_state_changes` to date cited changes against repairs; current closures omit it. `health_cleared_at` records the start of the final health-clear streak, without root-cause attribution. Program trust retains its separate bound application and NoMatch checks. The closure's `cleaned_helpers` (absent in older ledgers) lists the responder helper pods and Jobs (`sdo.dev/responder-helper=true`) the controller deleted when the responder completed. The closure's `incident_detector_states` (absent in older ledgers) holds the latest post-response evaluation of each non-health detector that raised a finding; it is learning evidence, not a closure gate. A closure with empty `final_detector_states` means health never cleared within the controller's verification window; the broker classifies it `failed` and strict receipt validation rejects it as a full mitigation. Reflection can still run for an independently verified partial repair, as described below. A missing proposal commit is valid under `recorded-actions`; current success classification uses successful repair actions, while verified clearing without a repair is `cleared_without_action`. Receipts do not re-check diagnosis or repair evidence. Require incident IDs and commit hashes to agree with the receipt and outcome.

`IncidentClosure.source_check_notes` and `OutcomeRecord.source_check_notes` list source or render checks that were unavailable when health verified the closure (accepted source would not render, the source inventory had not loaded). They are information only: the closure stays a success decided by health alone. The notes also list each object that still differs from accepted source ("unexplained drift remains on Kind/name"); an observed source difference no longer contradicts a closure. Absent in older ledgers and outcomes.

`IncidentClosure.frozen` is set when the controller persisted the closure, with the recheck's notes folded in, before the first broker attempt. The ledger's `closure` and the controller state's `pending_closure` must then be byte-identical; a retry withholds publication on active health and never rewrites the frozen payload or downgrades it to unconfirmed. `closure differs from persisted broker evidence in <fields>` names the differing top-level closure fields and fails the closure on its first attempt (non-retryable, reason `BROKER_ERROR`); with frozen closures it is a real controller defect, not live drift. In older runs (no `frozen` key), retries recomputed notes from later live state and hit this error after any interrupted attempt.



### Deployment plan (`.sdo/deployment/plan.yaml`)

The accepted tree declares what is deployed and how: `plan.yaml` is the `DeploymentPlan` (source renderer and config, image declarations, expected workloads) in compact ProtoJSON. It arrives in the deployer's `SDO-Actor: deployer` initial commit (Go writes it from the agent's typed plan, or from a provided `--deployment-plan`, before the deploy so the verified tree includes it) or in a human commit `sdo: record the deployment plan` (`SDO-Origin: install-existing`, from `install-existing --deployment-source`). The broker's source-evidence record (`<broker workspace>/source-evidence/<sha256(id)>.json`) no longer has a `plan` field: it holds the deployment source, `base_tree`, `effective_tree`, `patches_digest`, render and environment digests, images and resource IDs. A record that still has `plan` fails to load with a reinstall message (`ErrSourceEvidenceCarriesPlan`); so does a controller or `install-existing` on a tree without the file (`no .sdo/deployment/plan.yaml`). The controller takes no `--deployment-source` or `--deployed-manifest-path` flag; `ControllerConfig` fields 63 and 112 are reserved.

### Followed upstream source (`.sdo/goal/sources.yaml`)

An optional human goal-class file written only with the Human goal commit when the operator was launched with `--source-kind` and `--source-config` (the host and `operate` flags; `install.Config.SourceKind/SourceConfig`). Layout: `sources: [{path: ".", kind: git, config: {repository, ref, poll_interval}}]`; one source at `.` only. `config` is the watcher's proto in JSON/YAML form; an unknown kind or field fails at launch and at `memory.ReadSources`, and the shared check reports `sources.invalid` for a structurally bad file. No file means nothing is followed (every pre-existing app). The Human goal commit (`SDO-Actor: human`, `SDO-Phase: initial`) that adds the file also carries `SDO-Upstream: <sha>`, the accepted source commit it was based on, i.e. the initial upstream revision; the follower treats the newest `SDO-Upstream` trailer on the accepted first-parent history as the previous revision. A goal commit without the sources file never has the trailer. To see what a run followed: `git show <ref>:.sdo/goal/sources.yaml` and `git log --format='%H %(trailers:key=SDO-Upstream,valueonly)'` on the accepted branch.

An accepted upstream advance is a single-parent broker commit `sdo(advance-<sha12>): validated advance` with `SDO-Actor: upstream`, `SDO-Phase: advance`, `SDO-Upstream: <upstream commit>` and `SDO-Upstream-Base: <previous upstream revision, the merge base>` and `SDO-Mirror: exact|legacy` (`exact`: the accepted source outside `.sdo/` equals the upstream revision's before and after, so local differences live only in `.sdo/deployment/patches`; `legacy`: the source already differed, three-way merge); upstream history itself never enters the accepted repository (upstream commits live under `refs/sdo/upstream-source/<sha>`). It changes only files outside `.sdo/`, and its tree is the deployed tree in its source-evidence record (keyed by the advance ID; digests only, the plan is the accepted tree's `.sdo/deployment/plan.yaml`, which the advance carries unchanged), which becomes the accepted deployment evidence. A held revision produces no commit. An advance whose patch series does not apply to the candidate is held before any cluster change as `patch_conflict` (context changed or file gone), `patch_upstreamed` (the change is already upstream; retire the patch) or `patch_invalid`; the follower hold text (`sdo-upstream-holds`) names the patch, owner, failing step and the upstream commits that touched its paths. List advances with `git log --first-parent --format='%H %(trailers:key=SDO-Actor,valueonly) %(trailers:key=SDO-Upstream,valueonly) %(trailers:key=SDO-Upstream-Base,valueonly)'`.

### Historical Python broker fields

These fields describe retired Python ledgers, not current ProtoJSON:
`reflection_attempts` counts turns including backend failures;
`reflection_backend_error` is the last such failure;
`reflection_fresh_retry_attempts` counts fresh retries after validation rejection.
`reflection_session_mode` records `resume` (a responder session exists, which is every closure an agent handled) or `fresh` (only a closure the automatic stage resolved with no agent, logged as "no responder session"); a resume that cannot run is a `failed` reflection decision with `session_unavailable`, `session_handoff_invalid` or `session_handoff_timeout`, never a silent fresh session.
The rejected diff was `<sha256(incident_id)>.reflection-rejected.diff`.
`reflection_skipped_reason` explained deterministic reuse from verified exact or
learned-detector matches and trusted successful playbooks. Such a skip used zero
reflection attempts, empty usage, a `no_change` decision with the skip reason,
and no reflection commit, accepted detector paths or rollout. Verify those
historical claims against the incident's detector, action and outcome evidence.

## Operational memory layout (schema version 8)

Detectors are registered by per-detector manifests at `.sdo/diagnostics/detectors/<health|incidents>/<id>/manifest.yaml` (the directory name is the detector ID). Schema version 3 stores playbook references (`possiblePlaybooks`, detector `Playbooks`) as logical IDs. Schema version 4 adds the `deployment` class under `.sdo/deployment/` (kinds `deployment-plan`, `deployment-file` with ID `values|manifests|build`, `deployment-overlay` with slot ID `telemetry`, `deployment-patch` with the file stem as ID; patch files are `patches/NNNN-<slug>.patch`) and retires the never-written `.sdo/rendered/` class; the 3 to 4 migration only stamps the version. Schema version 5 adds the optional telemetry binding files (below); the 4 to 5 migration only stamps the version. Schema version 6 adds designer-owned data checks at `.sdo/diagnostics/data/<check-id>.yaml` (kind `data-check`; the 5 to 6 migration only stamps): the shared check installs a Go-owned health detector `data-<check-id>` per file, whose findings carry rules `data-check.shape`, `.bounds`, `.reference`, `.equivalence`, `.key-type` and `.too-few-records` and metadata `check`, `component`, `records` (keys only) and `sampled`; its pod reads are `data-check-<check-id>` and the lookup `data-check-<check-id>-lookup`. Rejections carry `data-check.invalid` or `data-check.component`. Schema version 7 adds the controller's `recovery_verification` to each outcome document (see outcome-records.md); the 6 to 7 migration only stamps, and earlier outcomes keep their `controller.recovery-unprobed` pending evaluations. Schema version 8 adds the optional `opening_keys` (the alerts the incident opened on) and `suspects` (the resources its findings named) to a corpus case's `labels.json`, which Go writes with the case; the 7 to 8 migration only stamps and leaves every earlier case valid without them. Deployment files are source bytes: a commit touching them is a deployer, human-seed, designer (overlay) or responder (add-only patch) change, so classify it by `SDO-Actor`/`SDO-Phase` trailers like any memory commit. Other schema versions fail fast; no format migration or retired outcome-stream import is provided. Readers must enumerate detector manifests instead of parsing one index. Schema version 5 also admits two optional detection-designer files, `.sdo/diagnostics/telemetry/sources.yaml` and `backends.yaml` (kinds `telemetry-sources`, `telemetry-backends`): application-telemetry bindings (source id, kinds, adapter, endpoint, credential name, readiness probe; designer-deployed backend recipes). "Application telemetry" is what the app reports and the backends storing it, not the controller's firing telemetry. Check them with `git show <ref>:.sdo/diagnostics/telemetry/sources.yaml`; a commit touching them is a `health_judge` `initial` change. Broker or `memory check` rejections carry rules `telemetry.schema`, `.adapter`, `.endpoint`, `.credential`, `.options`, `.default`, `.backend`, `.origin` and `.mode`, each naming the source and the fix (an endpoint outside the granted namespaces, an IP literal and an unregistered adapter are the usual causes); a pinned or floor environment rejects the files altogether. The controller reads them: the memory host built from the accepted revision reports `sources.yaml` in `Ready` (compiled into the host binary, so it is bound to the accepted commit and binary digest), and the controller rebuilds its metrics observer from the operator environment (`--telemetry-environment`, the `sdo-telemetry-environment` ConfigMap: provided source `default`, credential directory, mode) plus those bindings at every memory swap. Absent files are normal; when metrics or alerts are unavailable, check the environment document, the swap log and `source ... is not bound; bound: ...` errors before blaming the detector. The shared check does read them: the generated detector compile test fails `telemetry.unbound` (a query names, or defaults to, no bound source) or `telemetry.kind` (the named source does not serve metrics) against the effective sources (operator-provided plus the designer file), and the generic `metrics-alerts` health detector is installed only when an effective source has the alerts capability.

The playbook index `.sdo/playbooks/README.md` is generated by Go (since 2026-10-28): one sorted line `- [<title>](<id>/README.md)` per active playbook, titled by the README's first `# ` heading (the ID when it has none). The broker drops agent edits to it, leaves it out of replay patches and regenerates it on the merged tree whenever a publication changes a playbook, so it is never a `memory.conflict` path. Older repositories may list playbooks twice (an ID link plus a titled one) until their first playbook publication regenerates the index; count playbooks from directories, not index lines.

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
Every reflection, success included, is queued behind the closure acknowledgment (`reflection_queued`), so the closure never waits for the reflection agent; before this, a success closure stayed `CLOSURE_STATE_PENDING` for the whole reflection run and the next fault waited.
A queued reflection ends with one closure phase: `reflection_committed` only after the
broker found `reflection_commit` on the accepted branch, `reflection_failed` for a failed
decision,
or `reflection_without_commit`; ledgers before this rule logged `reflection_committed` for all three.
Frozen requests live under the ledger's `reflection-tasks/` directory.
`ControllerState.failed_closures` is an outbox, archived before the next state CAS
as content-addressed `sdo-failed-closure-*` ConfigMaps (`failed-closure.json`, label
`sdo.dev/closure-archive=failed`). Exhausted closures retain evidence and allow
later incidents; archives are runtime evidence, not operational memory.


For incident restart evidence, read [native incident recovery](native-incident-recovery.md).

### Per-cluster reachability baseline evidence

The opt-in healthy-baseline gate records fresh failed dial samples, captured while all other health detectors and the authored objective independently passed, as per-cluster controller state: `.sdo-runtime/reachability-baseline.json` at the lane's repository volume root (on fastloop lanes, the lane's host PVC directory, not the shared accepted checkout). It is not memory, is never broker-published and differs between lanes by design. Read version, authorization/source/objective bindings, capture time, exact targets, Service UIDs, configuration hashes and measured samples. A missing historical success alone never authorizes an exclusion. A changed source/objective/Service name or target/backend/config/policy invalidates the record for that target; a resulting finding reports `baseline_status`. A restart reuses the cluster's own record, including an empty record, and restored incidents cannot capture their failures as baseline exclusions. A gate that cannot measure logs `reachability baseline unavailable` and keeps running; until a later healthy measurement, the `service-reachability` evaluation reason lists never-served targets as unknown. Candidate failures in saved logs are not accepted exclusions without this measured healthy-baseline evidence.

Older runs committed this record into memory at `.sdo/diagnostics/detectors/health/service-reachability/baseline.json` (`reachability-baseline-…` broker subjects as `health_judge`/`initial`), one lane's capture at a time. Current code never reads that file; a health-judge initial check deletes it, and the `memory.cluster-local` validator rule rejects writing it or other cluster-local fields. Shared-memory attachment no longer tolerates any lane divergence, so attachment reports have no `discarded_cluster_captures`.

For partial repair, reflection publication and retirement evidence, read
[native reflection](native-reflection.md).

## Retired playbooks

Playbook README front matter may set `status: retired` with a nonempty string
`retired_reason`; omitted status or `status: active` is active. The stored README,
program source and immutable corpus remain evidence. Retirement removes the
playbook from worker snapshots, detector hints, generated program registration,
coverage, mitigation menus and the generated index. A README change reloads the controller registry.
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

For ordered stage reports and rollout handoffs, read
[ordered response stages](response-stages.md).

### Rollout receipt revisions

Skipped replay earns no origin credit. Default trust requires three independently
verified live successes and zero failures for the unchanged revision. Equivalent
ad-hoc repairs retain bound plan-equivalence receipts and post-repair NoMatch;
unrelated repairs in the outcome do not suppress credit. Every planned resource
needs a successful receipt, exact observed plan equivalence, and independent
outcome success. Match-to-NoMatch alone earns nothing. Summaries distinguish verification and mutation. Each program earns at most one
success per outcome; failed live fast paths roll back and block revision trust.

Validator-image identity is retired from current native receipts. Controller and responder image identities remain; historical `validator_image` fields do not establish current acceptance. Broker compilation executes the current controller binary in a bounded subprocess.


For every native controller, accepted-memory activation does not restart the
controller. Correlate `memory_host_activation` log commit/digest and
`activation_latency_ms` with the native ledger's matching accepted revision and
`memory_host_activation` phase. The temporary `rollout_receipts` wire field holds
host executable provenance in this mode; it does not prove a controller rollout.
`controller_revision` in the activation log remains the original controller build.
Never infer that a learned detector is live from acceptance alone: candidate
build/catch-up failures leave the previous host active and closure unacknowledged.
There is no flag-off path or restart supervisor. Startup and resume cannot record
a controller artifact as a host activation. Source-deployment receipts remain separate.

Memory-host observations contain optional input windows only. Kubernetes object
state is selected by the completed mirror sequence; no inline snapshot fallback
exists. A failed scheduler start leaves the previous revision serving, and crash
recovery restores the accepted immutable revision with its requested traffic state.

Saved-memory porting: use `sdo-operator memory migrate <checkout> --worktree-root <external-dir> --operator-root <operator> --replay on|off`. It shares startup Upkeep and registered schema migrations. Audit clean saved trees with `memory check --accepted --root <checkout>` and the original replay policy. An unchanged HEAD passed as a proposal base returns diff.empty; it is not an accepted-tree audit. Record source commit, replay policy and migration receipt. Migration never imports retired outcomes.jsonl.

Proposal checkouts for the deployer (initial) and responder (proposal) phases are materialized: the commit's `.sdo/deployment/patches` series is applied to the working files and `.git/sdo-effective-view.json` records the base tree and effective tree (checkout-private, never published). Edits to app files there are scratch; `sdo-operator patch create` captures them as the next `NNNN-<slug>.patch`, `patch check` reports `patch.uncaptured` files and patches that do not apply, and `deployment render` renders what Go would deploy. Reflection and import checkouts are not materialized.

## Operability report evidence

The Go broker records the operability report of each verified deployment and health commit in the evidence store next to the source evidence, never in `.sdo/`: `source-evidence/<sha256(operation id)>.operability-deployment.json` for the source commit and `.operability-complete.json` for the health judge's operation ID. The source evidence record carries `operability_digest` (the SHA-256 of the canonical report JSON) and the source commit carries the trailer `SDO-Operability: deployment sha256:<digest>`; the health commit has no trailer (its hash would depend on the report that names it) and its report names the commit in `subject.accepted_commit`. The stored report has no verdict: readers recompute `Decide`. `git log --format='%H %(trailers:key=SDO-Operability,valueonly)'` lists source commits with their report digests. A lane whose accepted source has no report fails with the reinstall message (`ErrSourceEvidencePredatesOperability`). An upstream advance carries the same: `.operability-advance.json` keyed by the advance ID (`advance-<sha12>`), `operability_digest` in its evidence record and the trailer `SDO-Operability: advance sha256:<digest>` on the advance commit; its stage-`advance` report (O1, O2, O4, O6) names the accepted head the advance was merged onto in `subject.accepted_commit` (the commit's parent). Reports never name an application, fault, benchmark or verdict.

The current report is also exposed as the `sdo-operability` ConfigMap in the control namespace (current state only, replaced at each install): `summary.json` is `{accepted_commit, stage, digest, observed_at, items:[{id, status, first_failed_check, cause, owner}], report_truncated?}` (status is the worst check of the item: fail, pending, pass, note, skipped, or missing) and `report.json` is the report (details dropped, then passing checks, when over 200 KiB; the digest is always of the full report). `kubectl -n <control> get cm sdo-operability -o jsonpath='{.data.summary\.json}'`. When the operate Job stops on an operability decision, its termination message is `{summary, decision, owner}` (decision `hold`, `correct` or `retry`) and the Job log has one `SDO-OPERABILITY {summary, decision, owner, report}` line.
