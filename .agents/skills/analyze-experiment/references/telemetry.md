# Telemetry and agent trajectories

## Agent trajectories

Generic CLI benchmark agents may emit JSON or JSONL trajectories under run-specific `agent/` or `trajectories/` directories. Their schema is agent-specific. Start by inspecting keys and timestamps rather than applying a historical fixed schema.

Use trajectories to explain hypotheses, commands, tool failures, and detours. Do not use them as substitutes for controller outcomes, broker commits, independent verification, or strict receipts.

## Codex command execution

Codex rollouts can encode commands as `event_msg` records with
`payload.type=item_completed` and `payload.item.type=CommandExecution`, as well
as legacy `response_item` tool calls and paired outputs. The item carries a
`command` argv array, commonly `["/usr/bin/bash", "-lc", "<script>"]`;
`payload.started_at_ms` and `payload.completed_at_ms` are epoch milliseconds.
Use these execution clocks rather than the record's export timestamp. Unwrap
the shell script and the `sdo act ... -- <command>` repair wrapper before
classifying writes. Ignore reads, pre-injection commands, malformed or
incomplete command items, and commands started after the mitigation POST; cap
completion at that POST. `incident_cost` accepts both schemas without changing
its output fields. A missing floor means no recognized command evidence, not
proof that no mutation occurred. Indirect writes inside Python or other scripts
can still require manual reconciliation.

## Worker provider resolution

Each SDO worker Job logs `sdo-agent-resolved role=<role> provider=<p> model=<m>` to its stderr and, where an incident session is mounted, appends the same record to `sdo_runtime/sessions/<digest>/agent-resolved.jsonl`. `run_validity` fails a run (`agent-provider`, `invalid_infra`) when a record differs from the manifest's configured provider/model for that role; runs without records report `not_recorded`.

## Detector firing telemetry stream

Current native outcomes omit `diagnosis_verification`. Their three classes depend on responder completion, health clearance and repair mutation, not independently confirmed citations. Historical outcomes may contain per-cause verdicts; interpret those under the earlier policy. Program trust consumes only controller-settled covered instances persisted before the outcome commit; historical outcomes without that proof add no success credit.

`agent/detector_firings.jsonl` (also `agent/sdo_runtime/telemetry/detector-firings.jsonl` and its rotated `.1`) is the controller's durable, append-only record of firing transitions, one JSON object per line with `schema_version` `sdo.detector-firing/v1`. It lives outside `.sdo/` and is not operational memory. Fields: `event_id`, `event` (`activated`, `cleared`, `not_persisted`, `batched`), `recorded_at`, `evaluation_iteration`, `application`, `namespace`, `detector_id`, `detector_class` (`health`/`incident`), `owner`, `rule_id`, `fingerprint`, `severity`, `parameter_bindings`, `surfaced_playbooks`, `firing_count`/`firing_threshold`, `clear_count`/`clear_threshold`, `incident_id` (empty when no incident is open), and `dispatch_relation`. An activation normally records `no_incident` (the batcher debounces) and a following `batched` record with the incident id settles `before_dispatch`. The stream is cumulative across a persistent pipeline, so scope by `incident_id` and time; a torn last line is possible after a crash, so skip unparsable lines. A `not_persisted` record with no later `activated` for the same fingerprint means the detector flagged the fault but it never met its firing threshold, which for a learned incident detector (required `persistence.firing: 1`) should not happen. `benchmarks/sregym/analysis/stream_curve.py` writes `detector_firing.csv` and the `incident_detector_fired_*`, `no_incident_detector_fired`, `fired_detectors` and `firing_telemetry` columns of `incidents.csv` from the receipt (falling back to the stream for incident ids); `firing_telemetry` empty means the run predates the telemetry.

Fastloop resolution rows keep the controller's `resolution_path` and `playbook_coverage` even if receipt validation later fails. An `equivalent-repair-*` plan application records a responder repair whose resources and final object fields matched a plan captured from a pre-repair program Match. Independent `success` is necessary for trust credit, and the stricter reader also requires controller-recorded plan/apply evidence with a post-apply `NoMatch` for the same binding and accepted dependency revision. An equivalent-repair record without that tool evidence earns no credit; see [program trust evidence](fastloop-results.md#program-trust-evidence).

## Incident lifecycle event stream

`sdo_runtime/telemetry/lifecycle-events.jsonl` (and its rotated `.1`) is the controller's durable, append-only record of its own incident lifecycle transitions, one JSON object per line with `schema_version` `sdo.lifecycle-event/v1`. Like the firing stream it lives outside `.sdo/`, is not operational memory, carries no benchmark labels or verdicts, is cumulative across a persistent pipeline (scope by `incident_id`), and may have a torn last line after a crash (skip unparsable lines). Absent in runs that predate it. It fills the lifecycle transitions the firing stream and outcomes never recorded, so an operator can reconstruct one incident end to end offline. Envelope fields match the firing stream (`event_id`, `event`, `recorded_at`, `evaluation_iteration`, `application`, `namespace`, `incident_id`); the `event` phases are `incident_opened` (with `finding_keys`, `surfaced_playbooks`), `workspace_prepared` (`worktree`, `base_commit`), `responder_dispatched`, `responder_completed` (`dispatch_error` when the responder Job failed terminally), `incident_closed` (`health_cleared_at`, `dispatch_error`), `closure_committed` (`proposal_commit`, `outcome_commit`, `reflection_commit` — join to the logical outcome by `outcome_commit` (individual JSON documents on active Go runs; JSONL in archived Python runs)), and `incident_acknowledged`. Each `(incident_id, phase)` occurs at most once and `event_id` is content-addressed over it, so a crashed-and-restarted controller re-emits the identical id and the stream drops the duplicate. To reconstruct a timeline, filter both streams by `incident_id` and order by `recorded_at` (lifecycle phases break ties in lifecycle order); a missing `incident_closed`/`incident_acknowledged` after an `incident_opened` pinpoints exactly where a stuck incident stalled.

Native worker task/result payloads use logical `playbook_id`, `script_names`, and `proposed_artifact_ids`; direct-edit reflection results reject the deprecated `artifact_proposal` and carry logical playbook IDs in `proposed_artifact_ids`. Older runs may include typed proposals; both the judge and reflection reject them on the current path. Resolve those IDs through the accepted-commit memory catalog and broker commit when investigating learning. Old Python-run receipts may still contain `.sdo/` paths in `playbooks`, `scripts`, or `proposed_memory_changes`; treat these as legacy transport values rather than extra artifacts. The worker's IDs and local check success are claims until the broker independently checks and accepts the actual checkout diff. The native broker ledger stores `reflection_rejections` (attempt, typed `MemoryDiagnostic` findings, and timestamp) before releasing rejected files, and marks `reflection_rejected` after the retry bound; two corrections are allowed, three dispatches total. A terminal rejection completes closure without a reflection commit and remains terminal after restart. Compare diagnostic rule/path/message and accepted Git changes; infrastructure errors do not consume the learning correction budget.

Fastloop stage checkpoints live under `results/<run-id>/checkpoints/<incident-number>/<stage>/`. `request.json` and `result.json` are the exact worker ProtoJSON; `checkpoint.json` holds capture digests, a digest of the direct file-edit bundle, and the settled native broker ledger. `file_edits.json` is recorded beside each exported worker request/result before publication and stored content-addressed by the checkpoint. Its version-1 edits carry repository-relative paths, base64 file bytes (null for deletion), and executable modes, bounded to 48 MiB encoded. Replay applies them to a fresh checkout and runs the real Go broker. Old proposal-only recordings fail with an explicit error; no proposal shim is kept. Corpus files live under `checkpoints/blobs/<sha256>` and are joined by digest, not by their original session path. A checkpoint is replay evidence, not an accepted commit or a benchmark oracle verdict. Default Codex fastloop registrations record provider CLI turns under the exported incident session's `model_io/<request-digest>/<prompt-digest>.json`; those are model I/O for deterministic plumbing replay, not additional agent decisions. Compare replay's broker acceptance with the recorded ledger and accepted commit before attributing a learning change.

## ConfigMap capture values

New controller corpus snapshots retain ConfigMap `data` and `binaryData` keys
with empty values, marked by `sdo.dev/configmap-values-redacted: "true"`.
These are key-existence observations; empty captured values do not establish
that the live ConfigMap was empty. Live detector read views are unchanged.
Previously accepted immutable corpus cases keep their original trusted bytes;
this policy does not retroactively sanitize old captures. Check programs that
read ConfigMap values before migrating their corpus or relying on new replay
cases. A value-dependent diagnosis needs a different supported observation.

## Responder Kubernetes API history (stage 1)

Per-incident session `api-history/` contains version-1 `manifest.json`, append-only
`events.jsonl`, and SHA-256-addressed projected response `blobs/`. This runtime
store is outside `.sdo/`; it is neither accepted memory nor replay evidence.
Envelopes identify epoch, request, start/end time, verb, GVR, namespace, name,
subresource, status, opaque resourceVersion and optional body hash. Hashes cover
projected bytes. A proxy-specific strict projection retains reviewed object
identity and pagination presence plus a positive per-kind operational allowlist:
Service ports/type; workload replicas, rollout type, template container images,
ports, probe shapes, resource budgets and reference names; Pod conditions,
readiness, restart/state reasons and exit codes; endpoint ready/not-ready address
counts and ports; Event type/reason/object/count/timestamps; and reviewed HPA,
PDB and NetworkPolicy spec structure. Label and match-label selector values become
empty strings; selector expressions expose only keys/operators. Secret/ConfigMap
payloads remain key names with empty values and no added labels. Service
`spec.clusterIPPresent` and Pod `status.podIPPresent` are derived booleans, not IPs.
Endpoints subsets and EndpointSlice objects expose `readyAddressCount` and
`notReadyAddressCount` (unspecified EndpointSlice readiness counts as ready).
Annotation contents, env values, command/args, probe exec commands/headers/hosts, all HTTP
probe paths (including segments, queries and fragments), mount paths and messages
stay opaque. Probe handler kind, port, scheme and timing remain visible. Image
references are parsed and normalized; invalid strings and URL credentials, queries
and fragments are omitted, while valid repository/tag/digest identifiers remain.
Condition, current/previous container-state and Event reasons outside the finite
reviewed vocabulary become `redacted`; this marker does not identify the omitted
reason. Constrained enums, quantities, CIDRs, named ports, timestamps and
downward-API expressions are validated/canonicalized. Bulk env references expose
only name/optional. Identifier names, label/selector keys, reference names/keys and
valid image identifiers are an accepted disclosure policy, not proof that a
user-chosen identifier cannot encode a secret. Existing blobs keep the policy of
the writer that recorded them. The single field-to-grammar table is
`sdo/operator/apihistory/identifiers.go`; the full per-field disclosure table is
in `docs/architecture.md`. Names/reference names/node names use DNS-1123
subdomains (Service object names use DNS-1035 labels); namespaces, container,
volume and Service/endpoint port names use DNS-1123 labels. Container named ports
and string target/probe/network-policy ports use Kubernetes named-port validation
(15 characters with a letter). Label/selector keys, referenced annotation keys,
Pod custom condition types and HPA metric/resource names use qualified names.
Data/reference keys use ConfigMap key validation; env names and reference kinds
use conservative C identifiers. Reference apiVersion is a DNS-1035 version label
with an optional DNS-1123 group prefix. Invalid leaves and keys are omitted.
Workload condition types retain only `Available`, `Progressing`, `ReplicaFailure`;
other types become `redacted`, including token-shaped custom types. Pod custom
conditions remain useful only when their types satisfy qualified-name syntax.
Custom HPA metrics/resources also require qualified-name syntax even when the
API server accepts broader strings. HPA target/described-object references expose
apiVersion/kind/name, never an invented namespace.

Request group/version/resource/namespace/name/subresource segments have the same
explicit DNS grammars described in the architecture table. Invalid request
names/subresources and nondecimal resource versions become field-separated
HMAC-SHA256 tokens (`opaque:` prefix), keyed randomly per history. The private
0600 `identity.key` preserves equality across recorder restarts; separate histories
produce different tokens. Numeric resource versions and valid request identifiers
(including known subresources) remain visible. Altered request identity makes the
envelope partial. Tokens allow equality comparisons, not recovery of raw text or
resource-version ordering; never interpret a missing identity as a wider scope.
Referenced annotation keys may appear through validated downward-API field paths,
without disclosing annotation contents.

Unknown fields and wrong JSON leaf shapes
are dropped. Every projected object keeps `redacted: true`; absence of a field
must not be read as evidence of its absence in Kubernetes. Label/selector keys
alone cannot establish selector equality or backend membership. Older blobs may
have only identity/key names; no store format migration is required. Readiness
gates, restartable-init semantics, exact probe requests/commands and individual
endpoint identities remain unavailable, so these fields cannot fully explain
every readiness failure. See
`sdo/operator/apihistory/projection.go` for the enforced per-kind allowlist and exclusions. Controller snapshot projection is
independent. Unknown representations, log bodies and streaming payloads retain
envelopes only. Body recording requires declared `application/json` (optionally
UTF-8 charset) and absent/identity content encoding; unknown or protobuf MIME and
compressed encodings remain unavailable even with JSON-shaped bytes. Request
bodies and headers are never stored; upstream error
logger text is discarded.

Default incident limits are 64 MiB and 10,000 envelopes, with a queue bounded by 1,024 entries / 8 MiB
and a 256 KiB projection budget. Up to eight transient workers project admitted
responses after forwarding; larger bodies or busy workers retain metadata and
mark a gap. Raw responses never enter the recording queue or storage. Deduplicated bodies count once toward the byte limit;
envelopes count per occurrence. `complete=false`, nonzero `dropped`, an open
writer, missing files, or a new epoch after restart prevent continuity claims.
Disk/queue/cap failures do not block forwarding. A missing manifest is Unknown,
including when the store itself could not initialize. The controller durably rotates
a sibling `api-history.generation` before each forwarding Job; manifests must match
that token. The sidecar also rotates it before recorder initialization. Missing,
unreadable, malformed or mismatched tokens remain incomplete after writer exit.
A failed controller generation write prevents launching the attempt. Runtime storage is removed

with the existing incident session lifecycle; there is no multi-session retention
policy. History cannot prove unobserved state or replace controller captures,
pre-apply checkpoints, action receipts, or independent recovery verification.

`evidence history` paginates envelopes; `evidence show --event N` prints the
redacted projected JSON or metadata; `evidence state Kind/ns/name --at CUT` reports
last-observed state, observation time, staleness and Unknown. CUT is RFC3339,
`before-first-mutation`, or `after-fix`. The first cut uses potentially mutating
request start times (including exec/attach/port-forward despite dryRun queries),
excludes only recognized supported dry-run writes, and cannot
be determined from incomplete history. `after-fix` requires a controller-written
`recovery.json` and compatible observations beginning after its verified time;
the last write response is never a recovery marker. Filtered/paginated lists,
unknown representations, crossing requests and metadata-only watches cannot
establish absence or continuity. A named GET 404 or complete scoped list can
record absence at the observation time only. Match the explicit GroupResource;
`Kind.group/ns/name` can qualify built-in kinds, while unsupported groups stay
Unknown. Cluster-wide list positives use item namespaces; they do not imply
broader absence.

Native responder Jobs run the proxy as a loopback native sidecar with the Pod's
existing service account. The proxy alone writes its incident history directory;
workers and fresh/resumed reflection mount it read-only. Exec, attach,
port-forward, watches and all logs retain redacted metadata only, without
truncating caller output. The
Go memory edge writes the verified recovery marker before reflection. Only
requests using the generated localhost kubeconfig are visible: direct network,
alternate endpoints and controller SDK observations are outside this history.

## Responder startup

The controller emits one `responder_startup` structured log record per incident worker launch. The `startup_json` field contains Kubernetes Job creation and Pod scheduling timestamps, each init container start/end, the agent container start, worker readiness, agent process start and first model request send time. Native restartable init sidecars have no end before the agent starts; `restartable=true` explains that null. Missing observations are explicit in `missing`; never substitute controller poll time for a Kubernetes timestamp. Codex first-request time comes from its local OTLP `codex.api_request` or `codex.websocket_request` event timestamp minus request duration, so it can arrive after the request finishes. This is the client request-attempt boundary, including transport/authentication setup, rather than first response time. Only timing fields are retained; the local collector discards other telemetry content. No model request event means the boundary is unavailable, including scripted workers that do not execute the model CLI.

Incident Jobs consume the image-bound read-only compiler seed prepared before controller dispatch; their init timings no longer include `prepare-toolchain`. Validator and reflection Jobs retain their own private compiler initialization.

Warm startup records explicitly carry `mode=warm`, `claimed_at`, `task_bound_at`, `pod_ready_at`, native `session_open_received_at`/`session_open_at`, `claim_to_session_open_ms`, `task_bound_to_session_open_ms`, `claim_to_first_model_ms` and literal `job_created_to_session_open_ms`. Cold starts carry `mode=cold` and a null claim boundary. Standby Job age is retained as provenance, never subtracted from a cold measurement or used as the warm claim denominator. The registered Python process may start before a warm claim; `registered_agent_process_started` is distinct from `codex_process_started`. Exactly one initial startup record is retained per incident; continuation turns do not reset its clock.

Native runtime-artifact export includes only the typed, exact session-level filenames `startup-controller.json`, `startup-worker.json`, `startup-daemon.json`, `startup-session.json`, and `startup-dispatch.json`; unknown JSON fields, unsupported timing sources and oversized timing files are rejected. These files distinguish preloaded imports, claim/binding, controller session opening and provider-request attempts without exporting the standby environment or inbox. A missing boundary stays unavailable, including first-request timing in scripted runs.

Controller startup, dispatch and session-open files live in the session's `startup/` directory, mounted read-only to the responder. Worker/daemon timing files stay at the session root. Native mode and claim/session clocks therefore cannot be rewritten by the responder. The exporter retains root-level exact legacy timing filenames as well as these three exact protected paths.

Warm recording workers normalize their local checkout alias to the canonical task checkout only for the model-I/O replay cache digest. The live request remains unchanged. Offline replay continues normalizing its temporary checkout to that same canonical task path; a warm encounter therefore replays without a live cache miss.
Early verified closure emits `incident_closed` at the controller's independent
verification time with `verified_while_responder_running=true` and
`closure_stability_window_seconds` (default 15). A new or reactivated finding,
observed mutation, pending receipt or unknown write outcome prevents that stable
pass. The response Job has a separate report grace (default 30 seconds); its
actual completion may follow `verified_at`. Outcome mitigation and completion
use the verification timestamp for these closures. Reflection waits for the
report or grace and retains the controller's receipts and resumable session.
The benchmark's ordinary grading remains after resolution/learning in
`fastloop/loop.py`; no grader signal enters controller verification.

The original task-bound clock is persisted before publication at `startup/attempts/<job>/startup-dispatch.json` and exported with the controller summary. Rejoining that immutable Job reuses its clock; a continuation receives its own clock. Derived durations are available only when both retained endpoints are ordered.

Per-Job native timings are exported at `sessions/<session>/startup/attempts/<job>/startup-controller.json`; worker clocks are at `sessions/<session>/startup-workers/<job>/startup-worker.json`. Initial incident files remain unchanged. A cold continuation keeps `mode=cold` and its own task-bound boundary; missing per-attempt boundaries stay unavailable rather than inheriting an earlier prompt's timestamps. Use the actual `responder_job_name` from native incident evidence for strict Kubernetes receipts, including warm Jobs. `--responder-standby=false` on install-existing/controller forces cold execution.

Incident execution discovery uses immutable control ConfigMaps named `sdo-exec-<task-digest-prefix>`, with a versioned `execution.json` binding incident ID, task ID, canonical request SHA-256 and actual Job name (plus the known warm Job UID). The index is published before worker release, independently of request ConfigMap/PVC transport, and survives request cleanup and Job TTL. Restart liveness and report-grace shutdown share the same index-backed inventory, with historical journal/session ownership recovery; API uncertainty retains incident ownership and its fence.

Native worker request checkpoints use the staged dispatch envelope: at most
64 MiB of exact UTF-8 ProtoJSON, shared by live recording and checkpoint
save/load, session/ConfigMap ingestion, and replay. A compiled contract test
compares the benchmark request budget with
`worker_registry.MaxStagedRequestBytes`. Result and broker-metadata evidence
remain capped at 1 MiB; native worker result transport retains its tighter
700 KiB cap. These budgets do not truncate memory, relax identity or digest
validation, or grant broker acceptance. A recorder rejecting a native-supported
request before the model runs is an **eval** failure; a failed reflection Job
or exhausted closure retries does not prove accepted learning. Preserve the
startup traceback and terminal native ledger separately from oracle grades.
