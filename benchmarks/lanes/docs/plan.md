# Plans

A plan is a declarative `plan.yaml` that the runner executes and recovers: arms x items x reps, edges that carry a
verified artifact, lane-free steps and pins. `lanes plan add FILE` validates it and stores it under
`<root>/plans/<name>/`; the manager (`lanes serve`) ticks every plan on each pass. The engine (`internal/plan`) sits
above the experiments: each node of a plan is one experiment (spec) that it adds to the manager, in plan order, once
its inputs are verified. All state is on disk, so a restart continues where the last pass stopped.

```yaml
plan: tp-gitlab-1                      # at most 24 characters; free again after `plan remove`
pins: {workload: fuzzops@4652656, subject: sdo@9b2cadb7f}    # written into every spec (fuzzops, operator.revision)
arms:                                  # declaration order is the order inside an item
  - {name: p1,   spec: specs/p1.json}  # spec template: ${plan} ${arm} ${item} ${item.<var>} ${rep} ${name} ${artifact.<kind>}
  - {name: cold, spec: specs/cold.json}
  - {name: p2,   spec: specs/p2.json}  # operator.seed: ${artifact.memory}
items: [gmem, gfeat]                   # or [{id: gmem, vars: {fault: "..."}}], or {from: items.json, select: key}
reps: 2
order: rep-major                       # rep 1 of every item before rep 2 (item-major: every rep of an item first)
parallel: 6                            # experiments released and not yet ended
isolation: two-way                     # optional; written into every spec
artifacts: {memory: {path: "${run}/operator/learned-baseline", verify: subject}}
edges: [{from: p1, to: p2, artifact: memory}]       # p2 of an item starts after p1 of the same item and rep verified
steps: [{name: merge, after: [p1], per: rep, run: "merge-memories", lane: none, out: "${step_dir}/merged"}]
stages: {pre-inject: 12m, post-inject: 25m}    # per-stage minimum; the runner alarms above factor x minimum
alarms: {factor: 2, idle_lane: 10m, arm_stall: 90m, hook: "notify-send lanes \"$ALARM_DETAIL\""}
```

- **Groups and spec overrides.** One plan can cover several apps. `groups:` names shared `vars` and a `spec` override; an
  item picks one with `group:` and may add `vars` and a `spec` of its own:
  ```yaml
  groups:
    gitlab: {vars: {pool: tp2-pool-g}, spec: {app: examples/gitlab, namespace: fg1, lane: {cpus: 8, memory: 30Gi}, hosts: [spokane1, chelan4], claim: "${item.pool}"}}
  items:
    - {id: gmem, group: gitlab, vars: {a: "..."}}
    - {id: gbig, group: gitlab, spec: {lane: {cpus: 16}}}     # merges into the group's lane map
  ```
  The override is merged over the arm's rendered template: maps merge key by key, every other value (numbers, lists,
  strings) replaces, `null` removes the key. Values keep their type; strings inside get the `${...}` variables (group vars
  are `${item.<var>}`, item vars win). The merged result is validated as a spec at `plan add` for every item (unknown
  fields and wrong types are refused, naming the arm and item), and an override may not set the engine-owned fields
  (`name`, `fuzzops`, `operator.revision`, `retries`, `isolation`). `items.from` entries take `group` and `spec` too.
- **Builtin operators.** `pins.subject` is optional. An operator with no hooks checkout (`operator.builtin`) has no
  revision: pin only the workload, and the engine writes no `operator.revision` (the spec check refuses a hooks operator
  without a revision, and a builtin one with it). Results then carry `subject_pinned: none`.
- **Pins by ref.** A pin may name a ref instead of a revision: `pins: {workload: fuzzops@HEAD, subject: sdo@origin/main}`,
  or with the checkout to ask, `sdo@HEAD:/abs/path/to/checkout`. `plan add` resolves it once with
  `git rev-parse` (in that checkout, or in the plan file's own directory) and stores the full revision in the plan, the lock and
  every spec, so a plan kept inside the pinned checkout does not lag one commit. Revisions (7-40 hex) work as before.
- **Checks at add time, and a submit that keeps failing.** `plan add` and `--dry-run` run the checks a submit makes, on
  every rendered spec: the spec itself, the inventory, the fuzzops binary on each host reports the pinned revision (#1612), `faults.instances` exists and holds every fault ID (`healthy`
  aside), `operator.baseline` is a directory (`operator.seed` that is an earlier arm's artifact is checked when that arm
  ends). A seed or baseline directory that exists gets SDO's memory check at the spec's pinned SDO revision (`sdo-operator memory migrate`, the controller's startup check); an invalid one is refused at add, naming it and the failed check. With no hooks checkout of the pin on the manager's host the check is reported as a note. A check that can still fail at release never loops silently: the backend's refusal is retried for
  `SubmitPasses` (3) passes in a row, then it raises a `submit` alarm and fails the attempt with reason `submit`; the
  same failure again parks the item. The checks of one session answer each probe once (the hooks checkout's describe and
  footprint, each host's pin, the input checks): the distinct specs of an add are checked together (at most 4 at a time, the
  hosts of a pin check at most 4 at a time), and the manager shares one session across the nodes it releases in a pass, so
  the first pass after `plan add` pays each probe once, not once per node. `plan add` also wakes a running manager
  (`<root>/wake`) so it does not wait out its tick.
- **Order.** Nodes are `(rep, item, arm)`, arms in dependency-then-declaration order, so every comparison's partner is
  released right after its pair, never behind a whole arm and never by name order. A node blocked on an input is passed
  over; the next ready node takes its slot, and the blocked one goes first as soon as it is ready.
- **Edges.** `from` is an arm (the same item and rep) or a step; `to` is an arm. With `artifact`, the source's artifact is
  exported and verified when the source ends and verified again just before the dependent is released. A failed verify
  fails the source (`artifact:<kind>`); its dependents are skipped. Verify modes: `exists`, `subject` (the subject's
  `verify-artifact` verb, falling back to `exists` when unsupported), `cmd:<shell>` (exit 0; `ARTIFACT_PATH`,
  `ARTIFACT_KIND`, `BENCH_EXPERIMENT` in the environment). Path templates: `${run}` (the source's newest done run),
  `${exp_dir}`, `${step_dir}`.
- **Steps.** A lane-free step runs detached, off any lane, once every node of its `after` arms (that rep's, with
  `per: rep`) has ended; it runs over the done ones (`LANES_INPUTS` JSON, `STEP_DIR`, `PLAN_DIR`). Its exit file, not a
  process table, says how it ended. Exit 0 is success.
- **Engine-owned spec fields.** `name`, `fuzzops`, `operator.revision`, `retries: 0` (only the runner retries) and
  `isolation`. Relative `faults.instances`, `operator.baseline` and `operator.seed` are made absolute against the plan
  file's directory.
- **Lock.** `lock.json` records the pins, the items and the hashes of the stored plan and templates; a changed copy
  refuses to run.
- **Retries.** Only the runner retries: every spec runs with `retries: 0`. Failures are counted per (item, reason),
  across arms and reps. The first failure reruns the node as a new attempt under a new experiment name (`-x1`, `-x2`);
  the second identical failure parks the item: the failing node fails for good, the item's unreleased nodes are parked,
  runs already in flight finish, and dependents are skipped. A different reason does not count toward parking. A bad
  artifact (`artifact:<kind>`), a spec that does not render (`spec`) and a vanished experiment (`lost`) are reasons like
  any other.
- **Alarms.** Raised once while the condition stands, written to `<root>/alarms.jsonl`, shown by `lanes status` (the last
  day, at most 10) and `lanes plan status`, and passed to `alarms.hook` (a shell command; `ALARM_JSON`, `ALARM_KIND`,
  `ALARM_PLAN`, `ALARM_KEY`, `ALARM_SUBJECT`, `ALARM_DETAIL`; 30 s limit; a failing hook is logged only). Kinds: `submit` (the backend refused a release for 3 passes in a row),
  `stage` (a stage took more than `factor` x its minimum, finished or still running), `idle-lane` (a free lane for
  more than `idle_lane` while runs wait, in the platform queue or held back by the plan), `arm-stall` (an arm with
  released or ready work and no run ended for `arm_stall`), `host-pressure` (a host over its `max_load5` or under its
  `min_free`). Liveness comes from the plan's own records and the lane and step records, never from a process table.
  Stage names are the backend's: the real one reports `queue`, `pre-inject`, `post-inject` for finished runs and, while
  running, the phase of the lane's step (`prepare`, `gate`, `window`, `undo`, `settle`, ...).
- **Results.** One row per ended attempt (done or failed) in `<plan dir>/results.jsonl`, written once and read back
  tolerantly: a missing or empty file is no rows, a damaged line is skipped (and counted on stderr), a repeated attempt
  keeps the later line. A row has the workload's grade (opaque JSON; it must be an object with a boolean `valid`, else
  the row is kept with `grade_error` and `valid: false`), the source identity (`source`: pins, the revisions the run
  reports, the app; `ok` is false when a reported git revision contradicts its pin), the isolation mode (`two-way`,
  `one-way`, `one-way-ok`), OOM kills, host and lane, and the stage times in seconds. A done run whose reported revision
  contradicts its pin, or a `two-way` plan on a run that was not two-way, is not a result: it fails with
  `source-mismatch` and runs again. A grade that is still being made delays the row, not the next run; a plan is
  complete once every row is written. The real backend grades a done run through the workload's grade verb in the
  background and reads the line for the run's operator label from `results/runs.jsonl`.
  An experiment that ran several runs on one lane (a `sequences` entry with several seeds, run in order with no reset
  between them) has one row per run, in run order, with a 1-based `position` (the TSV gets a `position` column only when
  a row has one); each run carries its own grade. A one-run experiment is one row, as before. A failed attempt is still
  one row.
- **Pause, remove, skip.** Requests are written to `<plan dir>/control.json` under a lock (never to `state.json`), so a
  command cannot race the manager and a restart loses nothing; the manager applies them on its next pass, idempotently.
  `lanes plan pause NAME` releases no new node and starts no new step (runs in flight are still watched and recorded);
  `resume` undoes it. `lanes plan remove NAME` cleans the plan up and frees its name: its experiments are removed (queued
  runs dropped; a run already on a lane finishes), they and the plan's directory (state, results, events) move to
  `<root>/plans/.removed/<name>-<time>/`, and the name can be added again at once, so a corrected plan replaces a wrong
  one without stopping the manager. With no manager running the command does it itself; otherwise the manager finishes
  it within a pass, and an experiment whose run is still on a lane moves when the run ends. `remove --keep` only
  retires the plan (waiting nodes and steps are skipped, runs in flight finish, the plan completes; directory and name
  stay). `lanes plan skip NAME ITEM` skips the item: its unreleased nodes (also those
  that come back for a retry) are skipped, in-flight ones finish, and a skip is not a failure (no parking).
  `unskip` stops skipping new nodes; those already skipped stay skipped. `--cancel` (`remove --keep`, `skip`) also cancels the
  runs in flight: the experiment is removed (its queued runs are dropped), the manager kills a run already on a lane
  (TERM, then KILL to everything the step started; the lane's cluster stays when the spec keeps lanes, and the lane
  is recovered like one whose step died) and running plan steps get SIGTERM; the cancelled node is skipped with the
  reason. Without `--cancel` a run in flight finishes, as with `lanes remove`. `plan status` shows
  `paused`, `removed, draining` and the skipped items. `lanes plan retry NAME ITEM|NODE` (or `NAME --all-parked`) un-parks
  after the cause is fixed: it clears the item's park and failure counts (a fresh first failure reruns again, a second
  parks again) and requeues its parked and failed nodes, and the nodes skipped because an input never came, with the
  same inputs: artifact edges are re-resolved and re-verified at release. The attempt number keeps growing, so
  experiment names (`-x<n>`) and result rows stay unique. It refuses a node that is done or in flight, and an item
  with nothing parked or failed. Like the other commands it writes only `control.json` (a request with an id); the
  engine applies it once, logs it in `state.json` (`retried`: when, and what the park was) and reopens a finished
  plan, so a manager restart loses nothing. `plan status` shows `retried: <node> retried at <t> (was parked: <reason>)`.
  Pins are locked in the plan, so a subject or workload fix needs `--repin subject=<rev|ref>` (and `workload=`; repeatable,
  `ref:/abs/checkout` names the checkout to resolve in): the value is resolved to a full revision now, every requeued node
  is rendered with it and checked like `plan add` checks a spec (`ValidateRendered`), and a refusal stores nothing. The engine
  gives only the retried nodes the new pin (`pins` on the node; `pin_changes` in `state.json` log role, old and new pin,
  request and nodes). Done nodes and rows already written keep their old pin, and a run on other source than its node's
  pin is still a source mismatch.
- **Commands.** `lanes plan add FILE [--dry-run [--json]]` (`--dry-run` runs every check of `add` -- parse, items, groups, every item's rendered spec through the spec validator and the inventory, a taken name -- and prints the items, arms, edges, steps and the nodes in run order with their experiment names; nothing is stored under the root), `lanes plan status [NAME] [--json]` (per plan: the state -- running, paused, removed, complete -- and an
  `items:` line that puts every item in one state: done, in flight, queued, blocked, parked, skipped or failed; per arm:
  done, in-flight, queued, blocked, failed, skipped, parked nodes; every parked, failed and skipped item with its reason;
  the alarms standing, with since when; the next runs and the next items; why the first blocked ones wait; `--json`
  adds `item_rows`, one per item with its group, state and reason), `lanes plan results NAME [--tsv] [--final] [--done] [--grade-fields a,b]` (JSON lines, or TSV with a header and a column
  per stage and per named grade field; `--final` keeps each node's last attempt, `--done` only done rows).
  `lanes status` prints one line per plan and the recent alarms.

Replaces the hand-rolled twopass drivers: `driver.py` (chaining pass 2 after a verified pass-1 archive, the `-x<n>` rerun
suffix), `mkspec.py`, `lanewatch.py` and `poolgate.py` (retry and park policy, stalled-lane detection), `collect.py`/`mkresults.sh` (`plan results --tsv`) and `tick.sh`.

## Commit series

The eval mode where SDO runs through a series of upstream commits is a workload scenario, not part of this platform: see [benchmarks/commit_series](../../commit_series/README.md). The platform only provides the generic pieces it uses (a multi-seed sequence is one lane run in order, and `plan results` has a row per run).
