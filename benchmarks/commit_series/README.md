# Commit series

An eval mode: SDO runs through a **series of realistic upstream commits** and we measure how it operates over time.
Engineers push to the branch SDO deploys from; SDO deploys and verifies each new source (operability report O1-O6),
holds refused heads and handles the incidents that arise. This is the mode the GitLab chart-release replay and the toy
scripted commits first ran in the fastloop harness; this package makes it available on the lane platform.

## Layers

| Layer | Piece | Knows |
|---|---|---|
| SDO | production, unchanged | only engineers' pushes to its branch (`install-existing --source-branch-url/--source-branch`) |
| Replayer | [`benchmarks/common/replay`](../common/replay/README.md): `replay`, `Series` | commit source, schedule, conflict policy; nothing of lanes, SREGym or fuzzops |
| Series spec | a JSON file, data only | where the commits come from and why they are realistic |
| Workload adapter | this package: `python -m benchmarks.commit_series <verb>` | walks one commit per run, records SDO's outcome |
| Subject hooks | `benchmarks/fuzzops/hooks.py`, `settings.source_branch` | creates the remote (`up`) and passes it to SDO; knows nothing of commits |
| Lane platform | [`benchmarks/lanes`](../lanes/docs/plan.md) | generic only: a multi-seed sequence is one lane, run in order, and `plan results` has a row per run |

A guard test (`tests/unit/test_architecture.py`) keeps lanes free of replay and commit-series code.

## How a series runs

One experiment, one lane, `sequences: [[1, 2, ..., N]]`: **position i pushes commit i**. SDO and the app live on the lane
for the whole series and nothing resets them between positions. For each position the adapter:

1. waits for the spec's schedule offset (from when position 1 ran),
2. pushes the commit with the replayer (`Replayed-From` trailer, never touching `.sdo/`; a conflict is recorded, and with
   `on_conflict: stop` the rest of the series cannot apply),
3. waits for SDO's verdict: **accepted** (an accepted commit carries the upstream SHA), **held** (typed cause and owner),
   **incident**, or **timeout**,
4. watches a quiet window, waits for an open incident to close (bounded), and
5. prints the record: outcome, `deploy_seconds` (push to verdict), `wall_seconds`, held cause/owner, `incident_opened`,
   `incident_resolved`, agent tasks, pod failures, controller relaunches, operability report items. `valid` is true for
   every outcome SDO can have; only an infrastructure problem makes a run invalid (which breaks the rest of the sequence).

A fault listed at a position in the spec (`faults: {"3": "<fault id>"}`) is injected after that commit through the
wrapped fuzzops workload (`fault_instances` in the instances file); its record is kept under `fault`.

Grading is per commit: the run's record is its grade (`grade` writes `report.md`, a table of commits). `lanes plan results
NAME --tsv --grade-fields outcome,deploy_seconds,incident_opened,agent_tasks` gives **one row per commit** (column
`position`). Every other verb (app up, gate, report of ordinary faults) is the wrapped fuzzops workload's, found through
`WORKLOAD_BUILTIN`.

## Using it

The spec for the experiment (`workload` is the adapter; the subject's `settings.source_branch` makes the remote):

```json
{"app": "examples/shop", "workload": "uv run --project <sdo> python -m benchmarks.commit_series",
 "operator": {"hooks": "sdo", "baseline": "<baseline made at the series base>",
   "settings": {"model": "stub", "source_branch": {"ref": "sdo-source",
     "up": "uv run --project <sdo> python -m benchmarks.commit_series remote up --kubeconfig {kubeconfig} --namespace {namespace} --ref {ref} --image {image} --repository {repository}",
     "down": "uv run --project <sdo> python -m benchmarks.commit_series remote down --kubeconfig {kubeconfig} --namespace {namespace} --ref {ref}"}}},
 "namespace": "<ns>", "faults": {"instances": "instances.json", "ids": ["toy-series"]}, "sequences": [[1, 2, 3]], "...": "..."}
```

`instances.json`: `{"ids": ["toy-series"], "series": {"toy-series": {"spec": "series.json", "verdict_s": 600, "steady_s": 60}}}`.
Do not pool the lane (a pool reset would not undo the remote's history). The baseline must be SDO's memory for the app
**as of the series base commit**: the first push is a diff against the base.

## Series

- `toy.py`: three commits to the toy app (two benign, one that fails backend readiness). Synthetic, for fast tests and
  smoke checks: accepted, accepted, held or incident in a few minutes.
- `gitlab.py`: the real upstream GitLab chart releases 10.4.2 (base) then 10.5.0, 10.5.1, 10.5.3, 10.5.4 (digests and the
  measured effect of each in `apps/gitlab/releases.json`), rendered through the lane's umbrella chart to one file per
  object so each commit's diff is what the release moved: all images on the minor release, images again on the patches,
  a label only on the last. That is the realistic upkeep a GitLab deployment faces. Built at run time from the archives.

Both are built by `build(dest, ...)` into a scratch directory outside the repository; no history is committed.
