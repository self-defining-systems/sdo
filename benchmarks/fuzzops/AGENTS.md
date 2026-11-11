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

## Run

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

## Rules

- Production code under `sdo/` never imports this package; this package uses SDO only through `sdo-operator` and
  the controller state ConfigMap.
- The diagnosis translation (`observe.classify`) is a keyword map from SDO's free-text root cause to fuzzops'
  vocabulary; the report keeps the raw text in `summary`. Report both when grading diagnoses.
- Run outputs (fuzzops results, observer snapshots, logs) go to the external `sdo-exp` directory.

## Helm packs and parallel pairs

- A Helm-based pack (for example GitLab) is rendered with `helm template -n <namespace> --no-hooks` into a
  plain-manifest pack, once per namespace: charts write namespace-qualified addresses into their ConfigMaps, so a
  render for one namespace wires another copy to the first one's services.
- For an app fuzzops already deployed, the same `install` adopts it: SDO applies the identical render.
- Several SDO+app pairs may share one kind node; `install` tolerates a concurrent install re-tagging the same images.
- SDO handles one incident at a time: between faults wait until the controller state has no open incident and no
  `pending_closure`.

## Evidence

- Save each pair's runtime evidence (responder sessions and Codex rollouts, lifecycle tasks, telemetry) with
  `sdo-operator runtime-artifacts` until SDO's native evidence sink lands: the observer seals a snapshot when an
  incident closes (`--operator-binary`, `--controller-image`), and `python -m benchmarks.fuzzops.evidence export`
  runs before any control namespace is deleted. Output goes to sdo-exp. `evidence.EvidenceTarget` is the hook for the
  sink's configuration.
