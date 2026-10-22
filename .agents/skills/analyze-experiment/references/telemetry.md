# Telemetry and agent trajectories

## Agent trajectories

Generic CLI benchmark agents may emit JSON or JSONL trajectories under run-specific `agent/` or `trajectories/` directories. Their schema is agent-specific. Start by inspecting keys and timestamps rather than applying a historical fixed schema.

Use trajectories to explain hypotheses, commands, tool failures, and detours. Do not use them as substitutes for controller outcomes, broker commits, independent verification, or strict receipts.

## Worker provider resolution

Each SDO worker Job logs `sdo-agent-resolved role=<role> provider=<p> model=<m>` to its stderr and, where an incident session is mounted, appends the same record to `sdo_runtime/sessions/<digest>/agent-resolved.jsonl`. `run_validity` fails a run (`agent-provider`, `invalid_infra`) when a record differs from the manifest's configured provider/model for that role; runs without records report `not_recorded`.

## Detector firing telemetry stream

Current native outcomes omit `diagnosis_verification`. Their three classes depend on responder completion, health clearance and repair mutation, not independently confirmed citations. Historical outcomes may contain per-cause verdicts; interpret those under the earlier policy. Program trust still requires controller-recorded application and post-repair NoMatch evidence.

`agent/detector_firings.jsonl` (also `agent/sdo_runtime/telemetry/detector-firings.jsonl` and its rotated `.1`) is the controller's durable, append-only record of firing transitions, one JSON object per line with `schema_version` `sdo.detector-firing/v1`. It lives outside `.sdo/` and is not operational memory. Fields: `event_id`, `event` (`activated`, `cleared`, `not_persisted`, `batched`), `recorded_at`, `evaluation_iteration`, `application`, `namespace`, `detector_id`, `detector_class` (`health`/`incident`), `owner`, `rule_id`, `fingerprint`, `severity`, `parameter_bindings`, `surfaced_playbooks`, `firing_count`/`firing_threshold`, `clear_count`/`clear_threshold`, `incident_id` (empty when no incident is open), and `dispatch_relation`. An activation normally records `no_incident` (the batcher debounces) and a following `batched` record with the incident id settles `before_dispatch`. The stream is cumulative across a persistent pipeline, so scope by `incident_id` and time; a torn last line is possible after a crash, so skip unparsable lines. A `not_persisted` record with no later `activated` for the same fingerprint means the detector flagged the fault but it never met its firing threshold, which for a learned incident detector (required `persistence.firing: 1`) should not happen. `benchmarks/sregym/analysis/stream_curve.py` writes `detector_firing.csv` and the `incident_detector_fired_*`, `no_incident_detector_fired`, `fired_detectors` and `firing_telemetry` columns of `incidents.csv` from the receipt (falling back to the stream for incident ids); `firing_telemetry` empty means the run predates the telemetry.

Fastloop resolution rows keep the controller's `resolution_path`, `fast_path_programs`, `fast_path_relation_covered_findings`, and `fast_path_skipped_reason` even if receipt validation later fails. An `equivalent-repair-*` plan application records a responder repair whose resources and final object fields matched a plan captured from a pre-repair program Match. Independent `success` is necessary for trust credit, and the stricter reader also requires controller-recorded plan/apply evidence with a post-apply `NoMatch` for the same binding and accepted dependency revision. An equivalent-repair record without that tool evidence earns no credit; see [program trust evidence](fastloop-results.md#program-trust-evidence).

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
identity, pagination presence, and Secret/ConfigMap key names with empty values.
All other values, specs, statuses, labels, annotations and diagnostic text are
opaque with explicit redaction markers. Controller snapshot projection is
independent. Unknown representations, log bodies and streaming payloads retain
envelopes only. Request bodies and headers are never stored; upstream error
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
