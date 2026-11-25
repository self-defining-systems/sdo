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
| Workload adapter | this package: `python -m benchmarks.commit_series <verb>` | runs one series event (a commit or a fault) per run, records SDO's outcome |
| Subject hooks | `benchmarks/fuzzops/hooks.py`, `settings.source_branch` | creates the remote (`up`) and passes it to SDO; knows nothing of commits |
| Lane platform | [`benchmarks/lanes`](../lanes/docs/plan.md) | generic only: a multi-seed sequence is one lane, run in order, and `plan results` has a row per run |

A guard test (`tests/unit/test_architecture.py`) keeps lanes free of replay and commit-series code.

## How a series runs

A series is a list of **events**, each either a commit or a fault, and **each sequence position runs exactly one
event**. One experiment, one lane, `sequences: [[1, 2, ..., N]]` with N the number of events. With no `events` in the
spec, position i pushes commit i. SDO and the app live on the lane for the whole series and nothing resets them between
positions (beyond the incident reset every run's collect makes).

The spec's `events` list (`benchmarks/common/replay/series.py`) gives the order: each item is `{"commit": <1-based
commit index>}` or `{"fault": "<fault id>"}`. Every commit appears exactly once and in order; faults go anywhere, several
in a row if wanted. For example a release, a fault on it, a second fault, then the next release:

```json
"events": [{"commit": 1}, {"fault": "web-oom"}, {"fault": "db-slow"}, {"commit": 2}]
```

with `sequences: [[1, 2, 3, 4]]`. The short form `faults: {"1": "web-oom"}` means a fault event right after commit 1
(one fault per commit); a spec gives `events` or `faults`, not both. `schedule` still gives one offset per commit and
times commit events only.

### A commit event

For a commit event the adapter:

1. waits for the spec's schedule offset of that commit (from when position 1 ran),
2. pushes the commit with the replayer (`Replayed-From` trailer, never touching `.sdo/`; a conflict is recorded, and with
   `on_conflict: stop` the rest of the series cannot apply),
3. waits for SDO's verdict: **accepted** (an accepted commit carries the upstream SHA), **held** (typed cause and owner),
   **incident**, or **timeout**,
4. watches a quiet window, waits for an open incident to close (bounded), and
5. prints the record (`event: "commit"`, `index` = the position, `commit_index`): outcome, `deploy_seconds` (push to
   verdict), `wall_seconds`, held cause/owner, `incident_opened`, `incident_resolved`, agent tasks, pod failures,
   controller relaunches, operability report items. `valid` is true for every outcome SDO can have; only an
   infrastructure problem makes a run invalid (which breaks the rest of the sequence).

It leaves `settle-work-only.json` in the lane directory: the position already waited (bounded) for its own incident, so
the platform's settle waits only for work in flight.

### A fault event

A fault event is the wrapped fuzzops workload's `run` of that fault (`fault_instances` in the instances file names the
fault file), exactly as a fault run outside a series: it injects, watches and grades the fault. Its record is the
position's run record, plus `series`, `index` (the position), `event: "fault"`, `fault_id` and `after_commit` (the
number of commits pushed before it). It writes no `settle-work-only.json`, so the platform's settle waits until SDO is
fully idle (incident closed, closing turn done), bounded by the run cap, and collect then takes the evidence and resets
the incidents, as after any fault run. The platform's safety check after the run (`run active`, then `run undo`) names
the series, not the position, so every position leaves its event in `series-event.json` in the lane directory and the
adapter answers that check for the fault the position injected (through the wrapped workload); after a commit event it
answers inactive. A fault SDO did not fix is therefore undone before the next push.

A fault is never injected after a release the subject **held** (the cluster then holds objects of a release it did not
accept, so the fault would measure that state). The held commit's record has `release_held` (`commit_index`, `reason`),
and the next fault event prints a record with `valid: false`, `comparable: false` and `invalid_reason: "release_held:
..."`, which ends the series at that position. With `"on_held": "continue"` in the spec the fault runs and its record is
labelled `comparable: false` with the same `release_held`. The next accepted release clears it; one that was not
accepted (a conflict, a timeout) does not.

The lane platform stores the instances file alone, so name the fault file (and the series spec) by absolute path; a
position whose fault file is missing or does not list every fault event of the series is refused before anything is
pushed.

### Grading

The run's record is its grade (`grade` writes `report.md`, a table with one row per event: a commit row has the commit,
outcome, deploy seconds, incident and agent tasks; a fault row has the fault id, valid, `recovered_in_window`, window
bad seconds and `last_bad_second`). `lanes plan results NAME --tsv --grade-fields
outcome,deploy_seconds,incident_opened,agent_tasks` gives **one row per event** (column `position`). Every other verb
(app up, gate, report of ordinary faults) is the wrapped fuzzops workload's, found through `WORKLOAD_BUILTIN`.

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
A lane may be pooled (`hold` + `keep_lanes`, claimed by the series; the reset between attempts costs minutes, not a rebuild)
when `settings.source_branch` also has a `rewind` command (`... commit_series remote rewind --kubeconfig {kubeconfig}
--namespace {namespace} --ref {ref} --head {head}`): the reset puts the branch back at the prepared head and drops SDO's
refused head, the golden copy returns the app to the base release. Position 1 then proves the lane equals a fresh install
(`verify_start`, plus the branch's head and SDO's hold) and runs nothing on a lane that does not; the series clock restarts
at position 1. The baseline must also name the series' Jobs: `python -m benchmarks.commit_series.gitlab baseline SRC DEST
--series series.json` derives it (a head push cannot amend the deployment plan). The baseline must be SDO's memory for the app
**as of the series base commit**: the first push is a diff against the base.

## Series

- `toy.py`: three commits to the toy app (two benign, one that fails backend readiness). Synthetic, for fast tests and
  smoke checks: accepted, accepted, held or incident in a few minutes.
- `gitlab.py`: real upstream GitLab chart releases (https://gitlab.com/gitlab-org/charts/gitlab), each with digest and
  measured effect in a release list under `apps/gitlab/`. Two lists, two series:
  - `releases-10.5.4.json` (the default): base **10.5.4**, the release our GitLab baselines (e.g. twopass
    `gitlab-std-v2`) were made at, then 10.5.5 (patch, 7 image references change), 10.5.7 (patch, 7), 10.6.0 (minor, 8
    images, 17 values keys added, 1 removed) and 10.6.1 (patch, 6 images, 2 keys). 10.5.6, 10.5.9 and others exist; the
    series skips some patches, as an operator who upgrades every few weeks would.
  - `releases.json`: base 10.4.2, then 10.5.0, 10.5.1, 10.5.3, 10.5.4, for a baseline made at 10.4.2 (none exists yet).

  Each commit's tree is the fuzzops GitLab pack rendered at that release (`helm template` of the pack's chart and
  values, plain-manifest conversion as the hooks do it) as `k8s/render.yaml`, so a commit is the diff one chart bump
  makes to the file the baseline's repository holds. Pod `checksum/*` annotations are left out (SDO's own
  instrumentation changes the config they hash, so they would conflict by construction). Checked: all four commits of
  the 10.5.4 series apply cleanly onto the `gitlab-std-v2` memory branch. Build: `python -m benchmarks.commit_series
  ...gitlab build DEST --pack <fuzzops>/examples/gitlab --cache DIR [--list PATH]`; new lists are measured with
  `... derive`. Built at run time from the archives (digest checked).

Both are built into a scratch directory outside the repository; no history is committed.
