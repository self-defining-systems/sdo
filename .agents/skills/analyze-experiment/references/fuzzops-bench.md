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
    operator/                                   collect: memory/.sdo/{outcomes,playbooks}, controller-state.json,
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

- Only `done` runs are graded. `invalid` reasons: `capped: <idle reason>` (SDO was still busy at the cap, often a
  queued reflection), `lane lost`, `inject failed`, a fuzzops invalid reason, `host interference`, `sequence broken`.
  Not-ready gates are lane events (in `gates.jsonl` and the lane timeline), not runs.
- An `idle` reason starting `alarm:` means SDO had an incident open; on a gate that is a false alarm while gating
  (`gates.jsonl` `alarm: true`). Other reasons: `busy:` (closure or reflection, or `startup baseline pending`: a new
  install's start-up period still holds openers until SDO has seen it healthy; the controller state's `startup`
  records `began_at`, `bound_at`, `established_at` or `bound_reached_at`), `down:` (controller pod not Ready or
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
  `transcripts missing` names incidents whose outcome says an agent ran but whose rollouts never reached the host.
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
