# Benchmark receipts and result evidence

## Strict production receipt

Path in an SREGym problem run: `agent/sdo_production_receipt_strict.json`.

The schema is `sdo.production-receipt/v2`, assembled by the benchmark adapter from native Go `incident-evidence` and correlated Kubernetes observations. The receipt embeds the raw `native_ledger`, the controller image and Pod UID, responder Job UID/success evidence, proposal/outcome/reflection commit hashes, outcome classification, and a resolution derived from that classification. Native reflection usage is in `native_ledger.reflection_decision.usage` and copied to top-level `reflection_usage`; the latter is the analysis fallback when rollouts are unavailable. `broker_mode=native` and `worker_mount_mode=proposal-only` are required. Validate it with `benchmarks.sregym.protocol.native_receipt.validate_native_production_receipt` before interpreting fields.

Native responder usage and its session ID are in `native_ledger.closure.result.{usage,responder_session_id}`; `native_ledger.outcome_document.usage` also records responder usage. Usage counts, including `model_requests`, are ProtoJSON int64 decimal strings; omitted counts are zero, while an omitted optional request count is unknown. The analysis tools accept these strings and legacy numeric counts, join the responder rollout by this session ID, and use receipt usage when rollouts are unavailable. Search both `sdo_runtime/sessions/<session-dir>/codex/sessions/YYYY/MM/DD/rollout-*-<session-id>.jsonl` and the legacy `sdo_runtime/codex/sessions/...` layout. Use only the matching responder's rollout for the TTM mutation floor; other retained incident and reflection sessions must not contribute.

For rollout command schemas and mutation-floor clocks, read [command execution telemetry](telemetry.md#codex-command-execution).

A valid v2 receipt correlates the request, result, outcome, receipt, responder Job and acknowledged ledger. It requires independently clear detector states, a `success` or `cleared_without_action` outcome, reflection when required, and a matching controller detector rollout. A benchmark verdict alone proves none of these. Current `resolution` distinguishes `sdo_mitigated` and `cleared_without_sdo_action` (historically also `external_recovery`); a rejected receipt labels an `unconfirmed` or `failed` outcome `sdo_unconfirmed` or `sdo_failed` (before RT1 both read `external_recovery`); diagnosis and repair citations are not independently checked. Count only the first as SDO mitigation. Interpret archived `sdo.production-receipt/v1` using its declared schema; never normalize it into v2.

Controller runtime artifacts are exported under `agent/sdo_runtime/`. Durable usage and telemetry streams remain cumulative across incidents, so scope by incident ID and timestamps. The export allowlist includes corpus snapshots, model I/O, session JSONL, and Codex/Claude rollout JSONL under `sdo_runtime/sessions/<incident-hash>/`; it excludes credentials and provider caches. Worker `checkpoints/<request-digest>/request.json` and `result.json` preserve dispatches even after ConfigMap cleanup, including failed reflection results. Model I/O records carry `turn_index`; repeated prompt hashes have separate occurrence files. Reflection replay reports per-proposal acceptance or exact rejection and any changed prompt hashes. A corpus hardlink export failure can leave an older run with only an early snapshot: check the resolution's stage-end artifact error before interpreting absent reflection I/O. The resolution's `stage_end_runtime_artifacts.export_seconds` times the controller-log and runtime-artifact exports (both cumulative, so they grow with the incident index). The source session directory is retained for seven days after durable acknowledgment, with daily pruning. A resumed reflection may share the responder session; use its turn timing and session ID to attribute cost, not the whole transcript. Persistent benchmark stages may publish a deferred receipt after the following stage drains the previous incident; require the receipt's incident ID and native ledger to agree before joining it to an oracle result.

For grading before reflection completes, see [closure grading evidence](native-reflection.md#closure-grading-evidence).

## SREGym result tree

Typical paths:

```text
third_party/sregym/logs/<run-or-pipeline>/
├── experiment_config.toml or pipeline_config.toml
├── run_manifest.json                       # launch provenance (runs from 2026-09-28 on)
├── pipeline_state.json                     # pipelines
├── lifecycle_seed_stage<N>/                # SDO pipeline reuse evidence
├── sdo_persistent_controller.json          # persistent-controller pipelines
└── stage_<N>_<name>/ or run root
    ├── experiment_config.toml
    ├── run_manifest.json                   # per stage, written when the stage starts
    ├── application_workspace/
    └── problem_runs/<problem-id>/
        ├── results_*.csv
        ├── agent/
        │   ├── sdo_production_receipt_strict.json
        │   ├── sdo_incident_resolution.json   # persistent-controller runs
        │   ├── sdo_rejected_production_receipt.json  # persistent runs whose drained receipt failed validation
        │   ├── detector_firings.jsonl      # controller firing telemetry (rotated file first), cumulative over a persistent pipeline
        │   ├── sdo_turn_usage.jsonl        # host-side lifecycle turns
        │   └── sdo_runtime/                # exported from the workspace PVC
        │       ├── usage/controller-turns.jsonl   # broker + reflection turns
        │       ├── usage/responder-turns.jsonl    # responder Job turns
        │       ├── telemetry/detector-firings.jsonl(.1)  # raw firing stream from the PVC
        │       ├── sessions/<session-dir>/codex/sessions/YYYY/MM/DD/rollout-*-<session>.jsonl
        │       ├── sessions/<session-dir>/claude/projects/...  # Claude provider only
        │       ├── codex/sessions/...             # legacy shared session exports
        │       └── controller_logs/<pod>.log      # kubectl logs --timestamps of each sdo-controller-run pod
        └── <harness and agent logs>
```

`run_manifest.json` (`benchmarks/sregym/runner/manifest.py`, `schema_version` 1) is written by the runner at launch into the experiment directory, the pipeline directory and each stage directory. Its fields:

- `kind`: `experiment`, `pipeline` or `stage`.
- `codex_prompt_appendix`: for each `agent = "codex"` config, `mode` (verify protocol), `exec_disclosure`, `allow_exec` (true when SREGym's agent proxy forwards `kubectl exec`, attach and port-forward for that arm; phase-1 exec parity) and `sha256` of the appendix text.
- `git`: `sha`, `dirty` and `dirty_paths`, plus `submodule` with `sha`, `recorded_sha` and `dirty`.
- `images`: keyed by `<role>:<ref>`. Each entry has `id`, `repo_digests` and `id_at_preflight`; agent images also carry the probed `codex` and `agentshim` versions and `import_error`.
- `versions`: `codex_cli` (`pin`, `host`, `stock_arm`) and `agentshim` (`pin`, `host`).
- `models`: one mapping per arm or stage, from role to `provider`, `model`, `effort` and `source`. The roles are `responder`, `reflection`, `lifecycle_deployer`, `health_judge`, `baseline_agent` and `sregym_judge`.
- `judge`: `model` and `effort`.
- `kind_topology`: `clusters`, `worker_nodes` and the observed `nodes`.
- `config`: `snapshot`, `sha256`, `source` and `source_sha256`.
- `host`: `load_average`, `cpu_count` and `disk_free_bytes`.
- `codex_quota` and `lanes`.
- `preflight`: `ok`, `mode`, `waived`, `checks` and `facts`.

A resumed run keeps its first manifest and appends each resume's manifest under `resumes`. Runs from before 2026-09-28 have no manifest.

Classify runs before analyzing them: `uv run python -m benchmarks.sregym.analysis.run_validity <dir>... [--logs-root third_party/sregym/logs] [--legacy] [--json out.json]`. It labels each problem run:

- `invalid_infra`, with reasons, for a harness or host fault. The causes are: no manifest or a waived preflight; no result row; a failed CLI install; exhausted Codex quota; tokens that do not reconcile between rollouts, usage records and result files; a passed stage without judge-free TTD/TTM, or a TTM before the TTD; an isolation-guard mismatch or no guard; another lane's kind nodes in agent output; an acknowledged submission the harness never graded; or a stopped pipeline that never published a stage's deferred receipt.
- `agent_failure` for a real failure. The causes are: a failed oracle, `agent_error`, no valid SDO strict receipt, or leftover `sdo.dev/responder-helper=true` objects.
- `valid` otherwise.

A directory name prefix a person gave (`invalid_`, `sdobug_`, ...) is shown as `manual:` but never decides the class. `--legacy` accepts runs from before 2026-09-28: a missing manifest is noted instead of failing, and a missing guard is accepted when the run did not overlap another run in time. `incident_cost` applies the same classification and excludes `invalid_infra` runs, printing each one with its reasons. Pass `--legacy-runs` for pre-manifest runs, or `--include-invalid` to report every run.

Current SREGym parallel runs instead write each run under `runs/<6-digit sequence>_<problem-id>/worker_<N>/results/`. A problem listed several times in `runner.problems` (kept in order, e.g. A B C D A B C D) and each of the `runner.repeat` independent attempts (passed as `--n-attempts`, expanded consecutively as A A B B; fresh fault injection and agent launch, not retries) gets its own sequence number, so group rows by `problem_id` across sequences rather than assuming one directory per problem.

`controller_logs/` is written by the adapter's cleanup, so it also exists after a failed controller Job; an export failure is recorded in `controller_logs/export_error.txt` instead of failing the run. Each controller evaluation line (`controller_iteration`, `findings`) carries no timestamp of its own; use the `kubectl` timestamp prefix. The broker ledger's `closure.request.detector_history` is frozen when the incident opens and compacted to the last 12 evaluations, so evaluations after dispatch appear only in these logs.

Every structured agent turn appends one JSONL record to the file named by `SDO_TURN_USAGE_LOG`: host-side lifecycle turns to `sdo_turn_usage.jsonl` in the run's agent log directory, and in-cluster turns to `/workspace/.sdo-runtime/usage/{controller,responder}-turns.jsonl` on the workspace PVC, which the adapter exports to `agent/sdo_runtime/usage/` before building the receipt. Each record has `recorded_at`, `provider`, `model`, `cwd`, `session_id`, `resumed`, `duration_seconds`, `tool_calls`, `shell_commands` (count), `shell_command_lines` (the commands in order, each capped at 2,000 chars; absent in older logs), `model_requests`, `model_requests_source`, and `usage` (`llm_calls`; agentshim's normalized token breakdown `input_tokens` (including cache reads and writes), `cache_read_input_tokens`, `cache_write_input_tokens`, `cache_write_1h_input_tokens`, `uncached_input_tokens`, `output_tokens` (including reasoning) and `reasoning_output_tokens`; the deprecated alias `cached_input_tokens` (cache reads); and `model_requests` when known). Records written before agentshim 0.7 lack `cache_read_input_tokens`, and on Claude their `cached_input_tokens` also counted cache writes. `llm_calls` is agentshim's turn count: always 1 per Codex `exec` run, but Claude's agentic turns. `model_requests` is the per-request count: for Codex, `token_count` events with usage that this turn added to its session rollout (`model_requests_source=codex-rollout-token-count`); for Claude, its reported turns (`claude-num-turns`); `null` when the rollout could not be read. Cross-check against the exported Codex rollouts, whose `event_msg`/`token_count` entries carry per-request `last_token_usage`; a resumed session's rollout also contains the earlier turns. Current runtime-artifacts exports retained sessions under `sdo_runtime/sessions/<incident-hash>/{codex,claude}/`; they are retained for seven days after durable acknowledgment and pruned daily. Match reflection to the responder session ID when resume mode was selected.

Result CSVs may flatten stage results into fields such as `Diagnosis.success` and `Mitigation.success`. They also carry conductor wall-clock epochs `fault_injected_at`, `diagnosis_submitted_at`, and `mitigation_submitted_at` (recorded when the agent's `/submit` request arrives, before API retries or oracles) and, for agents with `defer_fault_injection`, `fault_injection_deferred_seconds`. Report timing judge-free for every arm: time to diagnosis is `diagnosis_submitted_at - fault_injected_at`, and the headline time to mitigation is `mitigation_submitted_at - fault_injected_at` minus the diagnosis-grading wait `fault_injected_at + TTL - diagnosis_submitted_at`, but never less than when the agent's last state-changing command before the mitigation POST completed (from the exported Codex rollout). The unadjusted `mitigation_submitted_at - fault_injected_at` includes judge time and is supplementary only (`raw_incl_judge_s`). The CSV's own `TTM` column additionally includes the mitigation oracle and is not a headline number. Runs with `defer_diagnosis_grading = true` carry `diagnosis_grading_deferred = true`: the diagnosis is graded by the same judge in the background while the mitigation stage is already open, so their grading wait is zero, `TTL` is measured at diagnosis submission, and the CSV's `TTM` no longer includes diagnosis judging; compare raw `TTM` only between runs with the same marker. Require explicit true values and inspect `agent_error`; missing or malformed values are not success.

For multi-stage SDO pipelines compared against stock Codex, `uv run python -m benchmarks.sregym.analysis.incident_cost <sdo-pipeline-dir> --codex <codex-dir>...` prints per stage `ttd_s` and the headline judge-free `ttm_s`; the supplementary `raw_incl_judge_s`, `no_judge_s` (raw minus the grading wait, which is zero for `diagnosis_grading_deferred = true` rows), `first_mut_s` and `last_mut_s` (first state-changing command issued, and last one completed, both from rollouts); cumulative `ttm_s`; `incident_resolution_seconds`; oracle verdicts, responder and reflection tokens (reflection kept out of resolution time), a token-breakdown table (uncached input, cache reads, cache writes and their one-hour part, output, reasoning, raw total, cost-weighted total, USD, model requests) for every SDO stage and baseline problem with the pricing table version and date it used (see "Token metrics" in `benchmarks/sregym/AGENTS.md`), receipt `memory_reuse.warm_path` beside whether the responder rollout's user prompt carried the warm-path instructions, `.sdo/` detector and playbook counts, and token break-even against the per-problem Codex mean with and without the one-time lifecycle (`sdo_turn_usage.jsonl`, or `--lifecycle-usage` for a seeded lifecycle).

## Typed receipt rejection

Native receipt v2 is defined by `benchmark_receipt.proto`; its acceptance,
classification summary and evidence checks run in the benchmark-only Go receipt
checker. `sdo_rejected_production_receipt.json` keeps `validation_error`, the
structured `receipt` when representable, and exact `receipt_json` for malformed
transport evidence. Its generated `failure` has `incident_id`, `code`
(`CODE_INVALID_WIRE`, `CODE_REJECTED_EVIDENCE`, or
`CODE_CHECKER_UNAVAILABLE`) and `message`. A checker transport failure is
infrastructure evidence, distinct from rejected incident evidence; run validity
classifies it as `invalid_infra`, while evidence rejections remain `agent_failure`. Do not count
either as a verified mitigation. The drain returns a typed failure for that one
incident; receipt rejection alone does not stop the next injection. Closure
failure and drain scheduling are separate controller-state paths. If a persistent
controller is absent or its pod UID changed before drain, the adapter writes a
rejected receipt on the previous incident and permits the next stage to install
a controller; this rejection does not prove reflection completed. Correlate the
loss with the harness cluster-reuse log before attributing it to a controller
crash. Pod inventory command failures remain errors, not evidence of pod loss.
Flat reflection usage follows ProtoJSON: int64 token counts may be strings.

First-party incident rows include `reflection_evidence_gap_count`, copied from the Go incident ledger. Missing
historical values remain unknown. Count typed `decline_reason=evidence_gap` decisions to distinguish actual
learning declines from missing observations that did not block a program; see [outcomes](outcome-records.md).


sdo3 outcomes: see [fastloop results](fastloop-results.md#sdo3-infrastructure-outcomes-and-composites).
