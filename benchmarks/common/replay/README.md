# Commit replayer

A standalone benchmark-side component: it pushes a series of upstream commits to the branch SDO deploys from, one at a
time, the way an engineer would. SDO is unaware of it (it only sees pushes). The replayer knows nothing about lanes,
SREGym or fuzzops; workload adapters call it, and the lane platform (`benchmarks/lanes`) has no notion of commits
(`tests/unit/test_architecture.py` enforces both).

- `replay.py`: `replay(ReplayConfig)` applies each commit three-way onto the branch head in a scratch clone, commits it as
  `bench-replayer` with a `Replayed-From` trailer and pushes without force. `.sdo/` is never written. A conflict stops
  the replay or skips the commit (`on_conflict`). `replay_patch` pushes one patch file the same way.
- `series.py`: the series spec (data, below) and its loader.

## Series spec

A series is data. One JSON file lists where the commits come from, when to push them and what to do on a conflict; an
adapter walks it. Fields:

| field | meaning |
|---|---|
| `name` | short id |
| `description`, `source` | why the series is realistic and where the commits come from (free text, required) |
| `upstream` | `{repo, base, commits: [sha...]}`: a Git repository, the commit before the first, and the commits in order |
| `mode` | `diff` (default; needs `base`) or `tree` |
| `on_conflict` | `stop` (default) or `skip` |
| `on_held` | `stop` (default): a fault event after a release the subject held is not run and the series ends there. `continue`: it runs, labelled not comparable |
| `schedule` | optional offsets in seconds from the series start, one per commit, non-decreasing; they time commit events only |
| `events` | optional list of positions, each `{"commit": <1-based commit index>}` or `{"fault": "<fault id>"}`; every commit exactly once and in order. Absent: one commit event per commit |
| `faults` | optional short form of `events`, `{"<commit index, 1-based>": "<fault id>"}`: a fault event right after that commit. Not with `events` |

The adapter turns the spec into a `ReplayConfig` per commit (`Series.config(index, branch_remote, branch)`), so a
workload can push one commit, observe SDO, then push the next with the same code.
