# fuzzops adapter

[fuzzops](https://github.com/self-defining-systems/fuzzops) injects incidents into a running Kubernetes app, runs an
operator for a window and grades user impact, safety, durability and diagnosis against a no-operator baseline. This
package lets SDO be that operator in its real mode: SDO deploys the app once and keeps operating it while fuzzops
injects faults one after another. fuzzops stays SDO-agnostic; it sees SDO through its generic `exec:<label>`
operator and `run --skip-reset`.

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

## fuzzops-bench hooks

[fuzzops-bench](https://github.com/self-defining-systems/fuzzops) (fuzzops repository) schedules experiments on lanes
(one kind cluster each), gates every inject on the app's journeys and the operator's `idle`, caps each run, grades only
done runs and writes everything under one results directory (layout: `.agents/skills/analyze-experiment/references/
fuzzops-bench.md`). It runs `uv run -q python -m benchmarks.fuzzops.hooks <verb>` in the pinned SDO checkout:

| verb | does |
|---|---|
| `check` | no cluster access: `baseline.json` well formed, its namespace is `FUZZOPS_NAMESPACE`, the render matches its sha256, the bundle has the branch at the commit |
| `prepare` | plain-manifest pack from the live app (the namespace's one Helm release, Jobs and namespaces dropped, the pack's other files beside `manifests/`; or the pack's own `manifests/`), refuse a render that differs from the baseline's (key order, document order and null-valued keys ignored), attached pack in `$BENCH_LANE_DIR/pack`, `go build` of `sdo-operator`, `kind load` of `sdo-{controller,responder,script-tools}:<image_tag>` on the workers, namespaces and credentials (`--from-file`), node image digest names, the bundle branch cloned as `main`, production `install-existing` with the baseline's controller flags and the settings' model, effort and provider. An operator left in the control namespace is removed first. Last stdout line: `{"pack", "operator_version", "memory"}` |
| `idle` | two kubectl reads: controller pod (`job-name=sdo-controller-run`) Ready, `sdo-controller-state` written within 60 s (newest managedFields time; every controller step advances `step_count`, so a live controller writes it), and no open incident, pending closure or queued reflection. Busy reasons: `alarm: incident open <id>`, `busy: ...`, `down: ...`, `stale: ...` |
| `watch` | `observe.py` on the control namespace with `--fault $BENCH_FAULT` (none for a healthy run), sealing an evidence snapshot at closure |
| `collect` | best effort into `BENCH_OUT`: `memory/.sdo/{outcomes,playbooks}`, `controller-state.json`, `controller.log` since the inject, `sdo_runtime/` (evidence export), `collect.json` |
| `teardown` | delete the control namespace (waiting) and the `sdo-admin-<control namespace>` ClusterRole and binding; never the app namespace |

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

## Evidence sink

Pass `--evidence-sink-host-path <node dir> [--evidence-run-id <id>]` to `install` so SDO's trajectories, task records,
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

## Evidence

- Save each pair's runtime evidence (responder sessions and Codex rollouts, lifecycle tasks, telemetry) with
  `sdo-operator runtime-artifacts` until SDO's native evidence sink lands: the observer seals a snapshot when an
  incident closes (`--operator-binary`, `--controller-image`), and `hooks collect` runs
  `evidence.export` after every run, before any teardown deletes the control namespace. Output goes to sdo-exp. `evidence.EvidenceTarget` is the hook for the
  sink's configuration.
