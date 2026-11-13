# Evidence sink

When a run was installed with an evidence sink (operator config `evidence.host_path` or `evidence.volume`, set by
benchmarks as `SDO_EVIDENCE_HOST_PATH`; runs before the central config used `--evidence-sink-host-path` or
`--evidence-sink-volume`), its
trajectories live outside the cluster under `<sink>/<run-id>/` and survive namespace deletion. Read them there first;
`sdo_runtime/` exports and the repository PVC may be gone. The layout is `sdo.evidence/v1` (check `layout.json`).

- `index.jsonl`: one record per sealed incident or lifecycle task (`kind`, `id`, `path`, `sealed_at`, `sha256`,
  `outcome`, `cause_classes`, `detected_at`, `verified_at`, `acknowledged_at`; tasks: `agent_id`, `task_type`,
  `status`). A unit sealed again (reflection after closure) has a later record; the last one per `path` is current.
  Query with `jq` or DuckDB (`read_json_auto('index.jsonl')`).
- `incidents/<sha256(incident_id)[:32]>/`: `incident.json` (exact ID), `ledger.json` (broker ledger: closure,
  outcome, reflection), responder and reflection rollouts (`codex/sessions/**/rollout-*.jsonl` or
  `claude/projects/*.jsonl`), `*.jsonl` logs and usage, `scratch/sdo-reports/*/NNNN-{act,apply,mitigate}.json`,
  `checkpoints/`, `model_io/`, `corpus/`, and `seal.json`.
- `lifecycle/<task-key>/`: designer, deployer and judge tasks (`task.json`, `request.json`, `result.json` or
  `error.json`, rollouts).
- `usage/`, `telemetry/` (detector firings, lifecycle events), `controller/` (controller-process sessions).
- `config.yaml`: the resolved operator configuration (keys in `docs/config.md`) the controller ran under. Read it before comparing runs: a different `response.timeout`, `mode` or `agent.effort` explains a different result. A run without it predates the `sdo-config` ConfigMap.

Completeness:

- An incident directory without `seal.json` was still open when the run ended (or the namespace was deleted): its
  files are the prefix mirrored up to then, at most ~10 s behind. That is evidence, not a closed incident.
- `sha256sum seal.json` equals the index record's `sha256`; each file's digest is listed inside it. A file whose
  digest differs from `seal.json` changed after the seal; prefer the later record.
- The agent API recording is off by default (`agent.recording`, config snapshot in the run directory): with it off there is
  no `api-history/events.jsonl`, the agent's raw writes and execs appear nowhere in the evidence, and a missing recording is
  expected, not a sink fault. Check `agent.recording` in the recorded config before reading its absence as a gap.
- The sink copies only allowlisted evidence; caches and credentials are never there. With `agent.recording: true` the agent API recording is there
  only as the redacted `api-history/events.jsonl` and `manifest.json` (`incidents/<d>/api-history/` for the responder,
  `incidents/<d>/api-history-reflection/` for reflection, `lifecycle/<key>/api-history/` for lifecycle tasks); blobs,
  the identity key and the mutation fence never are. Each write has `start` and `end` events with the changed field
  paths (keys only), `field_manager` `sdo-responder` for responder writes, and for exec/attach/port-forward the
  container, a redacted `command` (`<withheld>` marks an argument outside the safe-token grammar) and `session_end`.
  A refusal is an `end` event with status 403 and omission `scope_read_only` (non-responder agents are read-only).
  A write the proxy could not record is answered 503 `recording_unavailable` and never reaches the API server, so it
  has no event at all.
- Every agent Job with a session records its CLI session on the session mount (`incidents/<d>/codex/...` or
  `lifecycle/<key>/codex/...`). Builds before the "durable agent session homes" fix ran a fresh-session reflection
  (stage-only closures, which have no responder session) with a pod-local `/tmp/codex`. A missing reflection rollout for such
  an incident on those builds is expected, not a sink fault.
- No sink configured means evidence lived only on the repository PVC. Deleting the control namespace deletes it.
