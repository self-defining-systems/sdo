# Evidence sink

When a run was installed with an evidence sink (`--evidence-sink-host-path` or `--evidence-sink-volume`), its
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

Completeness:

- An incident directory without `seal.json` was still open when the run ended (or the namespace was deleted): its
  files are the prefix mirrored up to then, at most ~10 s behind. That is evidence, not a closed incident.
- `sha256sum seal.json` equals the index record's `sha256`; each file's digest is listed inside it. A file whose
  digest differs from `seal.json` changed after the seal; prefer the later record.
- The sink copies only allowlisted evidence; API history, caches and credentials are never there.
