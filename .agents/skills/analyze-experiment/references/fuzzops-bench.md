# fuzzops-bench results

fuzzops-bench (fuzzops repository) runs experiments of fault runs on lanes (one kind cluster each). For SDO every
sequence starts on a freshly built lane: a fresh app copy, then SDO restored from the experiment's baseline by
`benchmarks.fuzzops.hooks prepare`. Each run is `fuzzops run --operator exec:sdo-<exp>-r<position>` after a ready
gate (journeys clean and the `idle` hook idle for the whole `ready_for`); then settle (`idle` twice in a row, within the
run's cap) and the `collect` hook. Later positions of a sequence run on the memory the earlier ones left.

## Layout (`<root>`, in sdo-exp, never in the repository)

```text
<root>/hosts.yaml, specs/<exp>.yaml             inventory and the user's experiment specs
<root>/inputs/<sha256>/                         content-addressed instances files and baseline directories
<root>/lanes/<lane>.json                        lane state (owner experiment, last prepared operator, reason)
<root>/lanes/<lane>.timeline.jsonl              {lane, step, kind: teardown|app|prepare|check|run|settle|collect|
                                                 safety|release|recover, start, end, ok, detail}
<root>/<exp>/spec.lock.yaml                     frozen identity (JSON): spec, input hashes, revisions
<root>/<exp>/gates.jsonl                        failed ready gates {run, lane, host, at, reasons, alarm}
<root>/<exp>/runs/<fault-slug>-q<seq>-r<pos>-a<attempt>/
    run.json                                    status done|invalid|dropped, reason, lane, host, operator_label,
                                                 fault (healthy for a healthy run), sequence, position, seed,
                                                 inject_at, settle_end, capped, fuzzops {revision, measure_version}
    record.json                                 fuzzops record: fault (none when healthy), operator, seed, valid,
                                                 invalid_reason, window, tail, lifecycle.inject_at,
                                                 ready.{for_seconds, waited_seconds, checked_at, misses}
    fuzzops.log, hooks.log
    exec/                                       the watch hook's FUZZOPS_OUT dirs (state-*.json, report.json,
                                                 timeline.jsonl)
    operator/                                   collect: memory/.sdo/{outcomes,playbooks,corpus,lib}, controller-state.json,
                                                 controller.log (since the inject), collect.json (per-part
                                                 ok/error), and from the lane's host evidence sink:
      evidence/                                 layout.json, index.jsonl (the interval's records),
                                                 incidents/<sha256(id)[:32]>/ (incident.json, ledger.json,
                                                 seal.json, rollouts under codex/sessions/ or claude/projects/
                                                 as rollout-*.jsonl, scratch/sdo-reports/...), lifecycle/<task>/
      transcripts.json                          {incidents: [{id, outcome, agents_ran, rollouts}], rollouts,
                                                 missing: [ids with outcome and agents_ran but no rollout]}
      incidents-reset.json                      receipt of collect's operator reset between problems
                                                 ({control_namespace, generation, acknowledged}); an incident still
                                                 open after the settle closed as RESET (no learning credit)
      build.json, host-load.json                build identity (SDO revision, image tag, baseline and seed commit) and host load at collect
      observability.json, observability/, applogs/   capture receipt (ok/absent/empty/error), Prometheus/Jaeger stores for the run's own window, pod logs (gzip)
      evidence-manifest.json                    sdo.cell-evidence/v1: grader, timelines, sessions with sub-agent files, items, problems;
                                                 `python -m benchmarks.fuzzops.cellevidence backfill <exp dir>` re-checks any cell
      view.json                                 dashboard view (fuzzops.view/v1): summary, hide, events, panels;
                                                 written by collect's `view` part or `python -m
                                                 benchmarks.fuzzops.view <run dir>`
      view/transcripts/<incident12>-{responder,reflection}.jsonl
                                                redacted fuzzops.transcript/v1 files built from the rollouts
<root>/evidence/...                             each lane's host sink (BENCH_EVIDENCE_HOST), one run directory
                                                 <exp>-<lane>-<build> per build; append-only, never cleaned by
                                                 the bench; the source of every run's operator/evidence/
<root>/<exp>/results/                           fuzzops-bench results: runs.jsonl (done records), report.md,
                                                 grades.json (fuzzops grade cells), runs.csv (fault, position,
                                                 seed, lane, host, status, reason, grade)
<root>/<exp>/sdo-table/                         SDO table: table.md, table.csv, false-alarms.json
```

A baseline directory holds `baseline.json` (format 2: namespace, optional control namespace, bundle, branch, commit,
render and its sha256, objective, a `config` overlay of operator settings beyond `sdo-bench.yaml`, the SDO revision it
was made with; a format-1 manifest's `controller_flags` are translated to `config` on read), the memory bundle and the
render. The `prepare` hook's argv, `SDO_*` variables, receipt and log (`install-existing-args.json`,
`install-existing-env.json`, `install-existing.json`, `install-existing.log`) are in the build step's output; the
overlay files it passed are under the lane's `sdo-config/`.

## Reading it

- Detector-first SDO rows record RFC 3339 `inject_at`, `sdo_page_at` (controller incident opening),
  `page_source` (`own` or `fallback`), and `fallback_page_at` (inbox write acknowledgement, or observed opening
  if the write acknowledgement is lost) in `operator/collect.json`. Fields with no observed event are null.
  `fallback_id` correlates the inbox write with its controller acknowledgement. The watch copy is
  `exec/*/sdo-page.json`. The 120 s default delay starts at the first harmed user second in `FUZZOPS_TRAFFIC`.
  These fields describe the row's first page. `episodes` lists every harm episode (the plain arm's rule,
  `repage.py`): `at` (first bad second), `sdo_page_at` (SDO opened its own incident for it: self-detected),
  `fallback_id`/`fallback_page_at` (the harness paged it 120 s after `at` because SDO had not), and `open_incident`
  (`own` or `fallback`: an SDO incident was already open in that episode before the page). `page_limit` is
  faults + 1, SDO's own pages included. `row_metrics` gives `harm_episodes`, `self_paged_episodes`,
  `external_pages`, `external_page_at` and `external_pages_after_open_incident`; vanilla rows give
  `external_pages` and `external_page_at` from `pages.jsonl`. Rows from before this record have them null.
- Vanilla Claude rows on a pin that exports `FUZZOPS_TRAFFIC` include `operator/onset-probes.jsonl`,
  the live samples of the run's own journey workload through the first bad bucket or window deadline.
  Use `operator/onset.json` for the hook's trigger time (`at`), the workload's `first_bad.Start`,
  and the exact `inject_at`. A null `at` with `done.json` saying no user harm was observed means
  Claude was never launched. Earlier hooks used a separate journey check and launched at a cap even
  when `at` was null. The final grader may backdate harm after checking for lost writes.

- A vanilla Claude row can hold several sessions: the agent is paged again when users are harmed after 15 s without
  a bad second (`benchmarks/fuzzops/repage.py`; at most faults + 1 pages). `operator/pages.jsonl` has one line per
  page (`paged_at`, `onset`, session id, times, exit, usage, cost, `users_recovered`: whether users had that quiet
  period afterwards; false means no further page could follow). `done.json` spans first to last session (`pages`,
  `agent_seconds`, `repage.stopped`: `window over`, `collected`, `page limit` or `unsupported` for a FuzzOps pin
  without `last_bad`); `usage.json`, `transcript.jsonl` and `session-dir/` cover every session. Totals are unknown
  when any session has no result event.

- Only `done` runs are graded. `invalid` reasons: `capped: <idle reason>` (SDO was still busy at the cap, often a
  queued reflection), `lane lost`, `inject failed`, a fuzzops invalid reason, `host interference`, `sequence broken`.
  Not-ready gates are lane events (in `gates.jsonl` and the lane timeline), not runs.
- An `idle` reason starting `alarm:` means SDO had an incident open; on a gate that is a false alarm while gating
  (`gates.jsonl` `alarm: true`). Other reasons: `busy:` (closure or reflection, or `startup baseline pending`: a new
  install's start-up period still holds openers until SDO has seen it healthy; the controller state's `startup`
  records `began_at`, `bound_at`, `established_at` or `bound_reached_at`, and while it holds `holders`: the firing
  openers, batched findings, unevaluated openers or warm-up that held the last pass, or since when it has been healthy;
  the reason reads `startup baseline pending (bound T): <holders>`), `down:` (controller pod not Ready or
  no state), `stale:` (state ConfigMap not written for over 60 s; every controller step advances `step_count`).
- A run owns `[inject_at, settle_end]`. SDO incidents belong to it when detected inside, or open at the inject and
  naming the faulted component. Each run's `operator/memory` copy holds every outcome of the lane so far; select by
  interval, never by directory.
- False alarms come only from `healthy` runs, plus incidents in a fault run only on objects its churn touched within two
  minutes (after churn).
- Grade cells are keyed by (fault instance, operator label); each position is its own cell, so first and repeats
  never average together. `--with` baselines (`noop`, `best-fix` builtin experiments) supply noop and best-fix cells.
- Before trusting a run, check `operator/collect.json`: a failed part (memory, state, logs, evidence) means that
  run's SDO-side columns may be incomplete, not that the run is invalid. An `evidence` part failing with
  `transcripts missing` names incidents with evidence of a dispatched agent task (`sink.agents_ran`) whose rollouts
  never reached the host; a watch-only run dispatches none.
- Transcripts (responder and reflection rollouts) live only in `operator/evidence/incidents/*/` (copied from the host
  sink); `operator/transcripts.json` gives per-incident coverage. The lane timeline's `transcripts` check after each
  teardown records `transcripts missing: N` when the sink lost any. Older experiments have `operator/sdo_runtime/`
  (in-cluster export) instead. `operator/view/transcripts/` holds redacted, normalized copies for reading (raw
  rollouts stay the source; `operator/view.json` `hide` keeps them from the dashboard).

## SDO table

`uv run python -m benchmarks.fuzzops.table <root>/<exp> [<root>/<exp2> ...] [--out DIR]` writes one column per
experiment: grade, bad seconds against noop and best-fix (`noop->best`: 0 = no better than noop, 1 = as good as the
reference fix), lost writes, harmful actions, lifecycle times from the grade cell, and from SDO's outcome records the
cause class, reach, tokens and cost (`None` for an unpriced model), `auto-applied` (`yes` = `resolution_path:
deterministic`, `partial` = a learned plan applied and the responder still ran) and the reflection decision when a
ledger is present. Repeats are compared with the first run of their sequence (users' recovery time, else escalation,
else SDO's closure time). The summary also counts churn findings, false alarms and alarms while gating.

The watcher's preliminary diagnosis uses findings whose keys appear in the incident request's
`opening_keys`. For older detector-opened requests without that field, it uses the available
findings after excluding `standing_keys` when present. External pages can supply a confirmed
responder diagnosis even when they have no opening findings.

## Row metrics for SDO and vanilla Claude

`uv run python -m benchmarks.fuzzops.row_metrics [--format markdown] RUN_DIR [RUN_DIR ...]`
prints JSON Lines by default. Each directory contributes a row, including invalid or incomplete
runs. Grader metrics come from `record.json`. Validity also honors the lanes status in
`result.json` or `run.json`. A reported recovery and an invalid grade can coexist.

- SDO page delay starts at the inject and ends at the own incident opening or the fallback inbox
  acknowledgement. An older row with only the opening timestamp uses that as its own page.
  Vanilla uses `done.json`'s agent start as its endpoint and marks the page source `n.a.`.
- The SDO headline starts at the incident opening and ends at the last evidenced fixing write
  for that incident. The tool looks for successful responder mutations and applied plan receipts
  before health clearance, or before recovery or the closing turn when clearance is absent.
  It uses the write's transcript timestamp, a correlated agent change or database update
  timestamp when available, or the plan application receipt. If none is available,
  `ttf_source` labels the fallback timeline marker. The fallback does not fill the
  evidence-based `anchor_to_last_write_s` column.
- Both arms' headline time to fix ends at the last fixing write: SDO from the incident opening,
  vanilla from the agent start (`ttf_s` = `anchor_to_last_write_s`, null when no fixing write was
  found). The vanilla session end, which includes the agent's own verification after the fix,
  is `agent_wall_s` (with several pages, the sessions' own time, not the waiting between them).
  For a recovered row, the tool searches calls issued through the final
  graded bad bucket. A database timestamp can place the exact write later than that bucket.
- An `external` row (an agent command, `benchmarks/fuzzops/external.py`) is read like a vanilla row; `done.json` `label` and
  `agent_version` name the agent and `session-dir/incident/` holds its own evidence.
- `first_episode_end_s` is the inject to the last bad second of the first failure episode, which
  ends at the first clean gap of 30 s; read it beside `inject_to_last_bad_s`, which isolated
  later blips (churn, one failed request) can extend.
- Arm `devops-agent` (AWS DevOps Agent, plan applied by a fixed applier) is measured like vanilla from its page; its
  fixing writes come from `apply.json` (`applied_plan_tool_start`), its cost from `usage.json`, and it has no tokens.
- When the transcript only brackets the write, the `*_tool_start` source gives the invocation's
  start as a lower bound and `last_write_tool_end_at` gives the result timestamp. The tool
  recognizes common Kubernetes and in-pod write commands. Shell scripts that hide their effects
  from these sources may require inspecting the evidence. The mutation cross-check looks for
  the same Kubernetes object during the call or within ten seconds after it. FuzzOps' Kubernetes
  polls cannot corroborate in-pod database writes.
- The evidence manifest lists read-only investigator worker directories in `investigators`,
  keyed by directory digest with the worker ID as value. Each ID identifies its parent incident
  via the `-inv-N` suffix. Investigator workers do not add to `incident_count` or
  `open_incident_count`. The row tool also recognizes their identities in older manifests.
- Usage sums the row's SDO response and closing phases separately. The `responder_*` totals
  include investigators. Native incident usage already includes them, so the row tool uses it
  once and falls back to known journal values where metrics are missing. `responder_usage_complete`
  marks whether the total is complete, and `response_sessions` lists each worker's parent incident,
  role, available usage and completeness. A start-only journal leaves that worker's cost unknown.
  SDO input tokens already include cache tokens. Vanilla input adds cache creation and reads to
  uncached input. Unavailable values are null in JSON Lines and a dash in Markdown.

When the evidence is only on another host, run the standard-library script there via SSH
without transferring the raw evidence. Replace the host and path as needed.

```bash
ssh evidence-host 'python3 - /path/to/experiment/runs/*/' < benchmarks/fuzzops/row_metrics.py
```

Prometheus evidence in `observability/prometheus.jsonl.gz` may contain multiple
`{metric, result}` rows for one metric. Join by the full series label set and
sample timestamp across consecutive chunks. Earlier captures stored each metric
in one row. The receipt counts distinct label sets across chunks.
