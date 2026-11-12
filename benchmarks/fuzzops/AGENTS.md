# fuzzops adapter

[fuzzops](https://github.com/self-defining-systems/fuzzops) injects incidents into a running Kubernetes app, runs an
operator for a window and grades user impact, safety, durability and diagnosis against a no-operator baseline. This
package lets SDO be that operator in its real mode: SDO deploys the app once and keeps operating it while fuzzops
injects faults one after another. fuzzops stays SDO-agnostic; it sees SDO through its generic `exec:<label>`
operator and `run --skip-reset`.


Between problems the hooks reset SDO's incidents (`sdo-operator incidents reset`): at the end of `prepare` and as the
last part of `collect` (after the run settled and its evidence is copied, before the next gate). A benchmark baseline
also passes `--self-heal-hold 60s` in its `controller_flags` (until the central config carries it).

## Pieces

- `pack.py`: from a fuzzops pack with plain manifests, the SDO application repository (manifests under `k8s/`),
  the `DeploymentPlan` (raw renderer, every workload expected) and the attached pack (same entry, journeys and
  app-specific faults; `up` only waits for the app, `down` refuses).
- `install.py`: provisions the namespaces and Secrets (credentials copied by `kubectl --from-file`, never read) and
  runs `sdo-operator host --deployment-plan` with the production agent flags (`--agent-provider`, `--model`,
  `--agent-effort`). No deployer agent runs; the detection designer and the controller do.
- `observe.py`: the command of `exec:sdo`. It polls `sdo-controller-state`, prints one line per SDO step (fuzzops
  records them as actions), writes `$FUZZOPS_REPORT` from the detector findings and then from the responder's
  confirmed root causes, keeps every state snapshot under `$FUZZOPS_OUT`, and exits after closure.
- `window.py`: which incidents belong to a run: detected in the window, or open at the inject with findings, causes
  or repair actions naming the faulted component by resource ref. `table.py` and `observe.py` (`--fault`) share it.
- `table.py`: SDO's table over a fuzzops-bench experiment (one column per experiment): joins the bench's grades
  (`results/grades.json`, `runs.csv`) with each run's collected outcome records (grade, bad seconds against noop and
  best-fix, lost writes, harmful actions, lifecycle times, cause class and reach, tokens and cost, auto-applied,
  repeat/first ratios per sequence, churn findings, false alarms from healthy runs and alarms while gating).
- `hooks.py`: SDO's operator hooks for fuzzops-bench (below).
- `view.py`: the dashboard view of a collected run (`operator/view.json`, below).

## fuzzops-bench hooks

[fuzzops-bench](https://github.com/self-defining-systems/fuzzops) (fuzzops repository) schedules experiments on lanes
(one kind cluster each), gates every inject on the app's journeys and the operator's `idle`, caps each run, grades only
done runs and writes everything under one results directory (layout: `.agents/skills/analyze-experiment/references/
fuzzops-bench.md`). It runs `uv run -q python -m benchmarks.fuzzops.hooks <verb>` in the pinned SDO checkout:

| verb | does |
|---|---|
| `check` | no cluster access: `baseline.json` well formed, its namespace is `FUZZOPS_NAMESPACE`, the render matches its sha256, the bundle has the branch at the commit |
| `prepare` | plain-manifest pack from the live app (the namespace's one Helm release, Jobs and namespaces dropped, the pack's other files beside `manifests/`; or the pack's own `manifests/`), refuse a render that differs from the baseline's (key order, document order and null-valued keys ignored), attached pack in `$BENCH_LANE_DIR/pack`, `go build` of `sdo-operator`, `kind load` of `sdo-{controller,responder,script-tools}:<image_tag>` on the workers, namespaces and credentials (`--from-file`), node image digest names, the bundle branch cloned as `main`, production `install-existing` with the baseline's controller flags, the settings' model, effort and provider, and always `--evidence-sink-host-path $BENCH_EVIDENCE_NODE --evidence-run-id $BENCH_EVIDENCE_RUN` (either unset: fails before any work). An operator left in the control namespace is removed first. Last stdout line: `{"pack", "operator_version", "memory"}` |
| `idle` | two kubectl reads: controller pod (`job-name=sdo-controller-run`) Ready, `sdo-controller-state` written within 60 s (newest managedFields time; every controller step advances `step_count`, so a live controller writes it), and no open incident, pending closure or queued reflection. Busy reasons: `alarm: incident open <id>`, `busy: ...`, `down: ...`, `stale: ...` |
| `watch` | `observe.py` on the control namespace with `--fault $BENCH_FAULT` (none for a healthy run); no PVC snapshot (the sink carries the evidence) |
| `collect` | best effort into `BENCH_OUT`: `memory/.sdo/{outcomes,playbooks}`, `controller-state.json`, `controller.log` since the inject (from the cluster), and from the host sink only `evidence/` (the interval's incidents and lifecycle tasks, relative paths kept, with their `index.jsonl` records) and `transcripts.json`; then `view.json` and `view/transcripts/` (below); last the operator reset between problems, `sdo-operator incidents reset` (open or escalated incidents close as RESET with no learning credit, memory untouched; receipt `incidents-reset.json`), so the next run's gate starts clean; `collect.json` |
| `teardown` | delete the control namespace with `--wait` first (its Pods get SIGTERM, so the evidence sidecar runs its final pass), then the `sdo-admin-<control namespace>` ClusterRole and binding; never the app namespace. The bench waits for `idle` before it |
| `transcripts` | read-only, 2 min, only `$BENCH_EVIDENCE_HOST/$BENCH_EVIDENCE_RUN` on the host: exit 0 when `layout.json` has schema `sdo.evidence/v1` and every `incidents/*/` with a `ledger.json` holds a `rollout-*.jsonl` (anywhere below it) unless no agent ran; else exit 1 with `no sink: <why>` or `transcripts missing: N (<short ids>)` first. The bench runs it at the end of every build check (the lane is not ready until it passes) and after every teardown (a failure is recorded; the reset proceeds) |

`BENCH_OPERATOR` names the baseline directory and the settings (`model`, `effort`, `provider` default `codex`,
`image_tag` default `fz-<revision[:8]>`, `auth_file` default `~/.codex/auth.json`). The operator images must already be
built on the host (`scripts/build_sdo_images.sh`); `prepare` fails clearly when one is missing.

### Baselines

A baseline directory (outside the repository, in sdo-exp) holds the accepted memory as a Git bundle, the render it was
made on and `baseline.json`:

```json
{"format": 1, "namespace": "<app ns>", "control_namespace": "<optional, default <ns>-sdo>",
 "application": "<optional, default ns>", "bundle": "memory.bundle", "branch": "<branch>", "commit": "<40 hex>",
 "render": "<relative path>", "render_sha256": "<hex>", "objective": "<health objective>",
 "controller_flags": ["--response-timeout", "1800s", "..."], "sdo_revision": "<commit it was made with>"}
```

Paths are relative to the directory. `controller_flags` are extra `install-existing` flags; the ones the hook sets
(namespaces, images, model, effort, provider, credentials, PVC, kubeconfig) are refused. The memory names its
namespaces, so a baseline restores only into its own namespace. `python -m benchmarks.fuzzops.hooks baseline
--kubeconfig F --context C --namespace NS --out DIR [--objective ...] [--branch B] [--pack P]` archives a running
lane's accepted memory (bundle from the controller's application repository) and its live render as a new baseline.
Never commit baseline data into this repository.

## Fresh install (to make a baseline)

Experiments run through fuzzops-bench and the hooks above. A fresh install (deployment plan, detection
designer) only makes new baseline memory; archive it with `hooks baseline` once it is accepted.

```bash
python -m benchmarks.fuzzops.install --pack <fuzzops>/examples/<app> --work <dir> --namespace <ns> \
  --control-namespace <ns>-sdo --objective '<health objective>' --controller-image sdo-controller:<tag> \
  --responder-image sdo-responder:<tag> --operator-binary <sdo-operator> --kubeconfig <kubeconfig> \
  --model gpt-6-luna --effort xhigh
export FUZZOPS_EXEC_CMD_SDO="cd <sdo> && uv run python -m benchmarks.fuzzops.observe --control-namespace <ns>-sdo"
fuzzops run --pack <dir>/pack --namespace <ns> --skip-reset --operator exec:sdo --fault '<instance id>' --window 600s
```

Baselines (`noop`, `codex`) run on a copy of the same pack deployed by fuzzops itself in another namespace of the
same cluster, so SDO never sees their namespaces.

## Dashboard view

`view.py` writes what the fuzzops-bench dashboard shows for a run: `operator/view.json` (`fuzzops.view/v1`, contract in
the fuzzops repository's `docs/bench.md`) and redacted `operator/view/transcripts/*.jsonl` (`fuzzops.transcript/v1`).
`collect` calls `view.write(dest)` last as the `view` part (a failure is recorded in `collect.json`, never raised);
`uv run python -m benchmarks.fuzzops.view RUN_DIR [RUN_DIR ...]` regenerates both for existing run directories and
rewrites nothing else. It reads only the collected copy (`evidence/`, `memory/.sdo/`) and the run's `run.json` /
`record.json` for the inject time and the run's incidents (`BenchRun.pick`; every incident of the copy without them).

- `summary`: incidents, outcome, cause class, reach, auto-applied, responder tokens, reflection decision, learned count.
- `events` (lanes `detect`, `stage`, `act`, `close`, `learn`) come from the ledger and outcome document; panels are the
  incidents table, repair actions, one transcript per responder and reflection turn, and what the reflection learned.
- Transcripts come from Codex `rollout-*.jsonl` `response_item` entries only, split at `task_started`; a turn whose
  prompt is about reflection is the reflection transcript. Codex runs carry no reasoning summaries today, so reasoning
  shows as "reasoning (not summarized)".
- `redact()` runs on every written string: secret-named key values, bearer/basic credentials, JWTs, API-key prefixes,
  PEM blocks, base64 data fields and the session's `creator_user_id` / `creator_account_id`. It errs on the side of
  redacting (a key such as `secret_namespace=x` loses its value). `hide` keeps the raw rollouts, `model_io` and the
  controller log from being served next to the transcripts.
- Limits live in the dataclasses (`__post_init__`); `write` clips and caps so the file is always valid.

## Evidence sink

Every fuzzops-bench lane has one, always (`sink.py`). The bench mounts a host directory (`BENCH_EVIDENCE_HOST`, never
cleaned by the bench) on every kind node at `BENCH_EVIDENCE_NODE` and names the sink run `BENCH_EVIDENCE_RUN`
(`<exp>-<lane>-<build step id>`, constant for the build). `prepare` installs SDO with that node path and run ID, so
rollouts, task records and broker ledgers reach `$BENCH_EVIDENCE_HOST/$BENCH_EVIDENCE_RUN/` while the run goes (at most
10 s behind; the SIGTERM pass of `teardown` flushes the rest). `transcripts` and `collect` read only that host
directory. `collect` copies an incident when it was detected, verified, acknowledged, reflected or sealed in
`[BENCH_INTERVAL_START, BENCH_INTERVAL_END]` or was open during it (an incident without a ledger: when its files changed
since the start), and a lifecycle task sealed or written in the interval. `transcripts.json`:
`{"incidents": [{"id", "outcome", "agents_ran", "rollouts"}], "rollouts", "missing": [ids with an outcome and an agent
run but no rollout]}`.

"No agent ran" (`sink.agents_ran` is false) is decided conservatively from the ledger: the incident has a closure whose
responder result is absent because dispatch failed, or has no `responder_session_id`, a `resolution_path` other than
`responder`, no usage and no response loop; the outcome document records no responder resolution, usage or loop; no
reflection was dispatched (`reflection_dispatch_at` unset, phase not `DISPATCH`); and every reflection result has no
usage and failed before its session started (`session_unavailable`, `session_handoff_invalid`, `request_too_large`).
Anything else, including an incident with no closure yet, counts as an agent run that needs a rollout.

For a manual `install`, pass `--evidence-sink-host-path <node dir> [--evidence-run-id <id>]` so SDO's trajectories, task records,
ledgers and usage are mirrored out of the control namespace while the run goes (layout: `docs/architecture.md`,
"Run evidence sink"). On kind the directory must be a node mount made when the cluster is created, for example
`extraMounts: [{hostPath: <sdo-exp run dir>/evidence, containerPath: /opt/sdo-evidence}]` with the host directory
`chmod 0777` (the sidecar runs as uid 65532). Deleting the namespaces then leaves `<host dir>/<run-id>/` intact.

## Rules

- Production code under `sdo/` never imports this package; this package uses SDO only through `sdo-operator` and
  the controller state ConfigMap.
- The diagnosis translation (`observe.classify`) is a keyword map from SDO's free-text root cause to fuzzops'
  vocabulary; the report keeps the raw text in `summary`. Report both when grading diagnoses.
- Run outputs (fuzzops results, observer snapshots, logs) go to the external `sdo-exp` directory.

## Watch-only

`install --watch-only` installs SDO with `--frozen-memory --watch-only`: detection, incident opening, verification
and closure run as usual, but no automatic stage, responder or reflection runs and accepted memory never changes.
Use it to measure detector precision (incidents opened on a healthy app) and detection/localization per fault
without agent cost; the incident record is the controller state, its lifecycle telemetry and the broker ledger.

## Helm packs and parallel pairs

- A Helm-based pack (for example GitLab) is rendered with `helm template -n <namespace> --no-hooks` into a
  plain-manifest pack, once per namespace: charts write namespace-qualified addresses into their ConfigMaps, so a
  render for one namespace wires another copy to the first one's services.
- For an app fuzzops already deployed, the same `install` adopts it: SDO applies the identical render.
- Several SDO+app pairs may share one kind node; `install` tolerates a concurrent install re-tagging the same images.
- SDO handles one incident at a time: fuzzops-bench injects only after `hooks idle` (no open incident, pending
  closure or queued reflection) held for the whole ready gate.
- `hooks prepare` renders a Helm-based pack generically from the namespace's one release (`helm get manifest`), so
  any Helm pack works; the namespace is part of the render, which is why a baseline restores only into its own.

## Evidence without the bench

- The bench hooks never take evidence from the cluster: a `runtime-artifacts` snapshot sealed on the repository PVC is
  lost with the control namespace. `evidence.py` (`sdo-operator runtime-artifacts` export, and the observer's
  `--operator-binary` closure snapshot) remains for manual pairs without a sink; export before deleting the namespace.
  Output goes to sdo-exp.
