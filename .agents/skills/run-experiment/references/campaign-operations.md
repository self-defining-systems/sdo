# Running a benchmark campaign

How to run a campaign of many problems across lanes, hosts and arms, organized by the stages of its life cycle. Each rule comes from a mistake that cost hours; most were repeated because the lesson lived in one account's memory. Host facts belong in the host state file, not here.

## Maintain this file

- When a campaign mistake costs time, or the user corrects how experiments are run, add or sharpen a rule in the matching stage in the same turn, and land it on main. Account-local memory is not enough: other accounts, threads and subagents read the skill, not your memory.
- Write one rule per mistake class, stated as what to do. Merge with an existing rule rather than adding a near-duplicate.
- When you measure a new stage time, update the reference timings.

## The in-principle minimum: the yardstick for every stage

Before launching, write down the least time each stage needs if nothing is wasted. It is a sum of the parts that must happen:

- **A problem:** inject + detect + the agent's own work (diagnosis and mitigation) + the oracle's verification window + required tail or learning step. Setup is not part of it on a warm lane.
- **A lane:** the one-time setup, but only when no warm lane, pool or snapshot exists.
- **A series:** `ceil(problems / lanes) × per-problem minimum`, plus serial dependencies such as pass 2 waiting for pass 1's learning. Add one lane setup only for cold lanes.

Then compare measured wall time with it, per problem and per series, as results land.

- **Actual above about 2× the minimum is a bug to find, not a fact to report.** Example: the agent diagnoses for 10 minutes but the problem takes an hour, so 50 minutes went to something other than the experiment. Break that time into stages from the cell's own timestamps and remove or shrink the largest one.
- Report the ratio (actual / minimum) per stage in every progress digest. It shows overhead the per-row numbers hide.

## 1. Plan

- Align with the user first: questions, arms (system, baselines, controls), metrics, protocol and sample size.
- Size the first round to give trustworthy results within one to three hours, using the series minimum above. Keep rounds small (one parallel wave), look at the results, then decide the next.
- Write the expected time of each stage (preflight, build, lane setup, each problem phase, series) in the run directory. Record actual start and end times as you go.

## 2. Set up

- **Read each host before placing work:**
  - load per core;
  - threads in disk sleep (`ps -eLo stat | grep -c ^D`);
  - memory pressure and `MemAvailable`;
  - free local disk;
  - other experiments' clusters already there.

  A host with hundreds of threads in disk sleep is stalled even if its CPU looks free.
- **Separate experiments that share a host in both directions:** each gets its own cpuset or slice and a load gate. One side's cpuset does not keep the other side's unpinned lanes off its cores.
- **Stagger lane creation:** at most about three image imports at a time per host.
- **Disk:** keep lanes, images and evidence on local disk, never NFS. Keep a disk watcher on every host.
- **Start every lane from an installed baseline, warm pool or snapshot**, never a fresh install per lane, unless install is the question. When each arm needs different memory, swap memory into a warm lane instead of rebuilding.
- **Run drivers under systemd.** Start every long-lived driver, feeder or loop with `systemd-run --user --unit=<name> --collect -p Restart=always -p RestartSec=30 -E PATH=$PATH -E HOME=$HOME --working-directory=<dir> -p StandardOutput=append:<log> <cmd>`.
  - Processes started from an agent session die when the session is recycled; `setsid nohup` does not escape its cgroup.
  - Restart with `systemctl --user restart`, never kill and relaunch by hand.
- **Smoke a new pipeline with several parallel cells**, watched live. Change one layer at a time (runner, SDO build, memory, lanes, dispatcher), and keep the last working pipeline producing rows meanwhile.

## 3. Run one problem

- **Break every cell's wall time into stages** from its own logs:
  - lane claim or reset;
  - app health and readiness gate;
  - inject;
  - detect;
  - agent work;
  - verification;
  - tail or learning;
  - cleanup.

  Compare each stage with its minimum. Setup or gates taking longer than the agent's work is the usual sign of waste.
- After the first cell of each kind, read its row and logs before trusting the rest.
- **Stop a failure loop early.** The same failure on two or more lanes or hosts is a pipeline bug, not lane flakiness. Park that arm after the second repeat and find the root cause on recorded evidence. Automatic lane rebuilds on repeated pre-injection failures only burn lane time.

## 4. Run a series: throughput and parallelism

- **Measure throughput as graded rows per hour, overall and per lane.** Compare it with `lanes / per-problem minimum`.
  - When adding lanes does not raise rows per hour, parallelism is not helping. The usual causes: a host bottleneck (load, disk sleep, memory, image pulls), a shared serial step (build gate, registry, one driver loop, a lock), or a dependency chain.
  - Find the bottleneck before adding more lanes.
- **Never let lanes idle while work is queued.** Relaunch after a failed gate, and fill idle lanes first.
- **Spread across every quiet host.** Move off a failing host within about 15 minutes, and delegate the host repair.
- **Queue baselines independently.** If baseline cells are queued only as twins of treatment cells, a treatment stall silently stalls the baseline too.
- **A fix blocks only the arms that depend on it.** Keep independent arms running.
- **Respect serial chains.** Example: pass 2 waits for pass 1's learning. Start chains early and run independent work beside them.
- Run all arms at once on the same problem set, never one arm after another.

## 5. Oversee

- **Watch from the first minute, not after a stall.** Schedule a recurring check every 20 minutes. Each check sends a subagent through a written checklist kept next to the driver:
  - rows graded in the last window against the window before;
  - the actual / minimum ratio per stage;
  - every failure, with its cause read from the cell's own logs;
  - stuck cells and idle lanes;
  - host pressure, disk, and whether drivers are alive.

  The orchestrator acts on the findings and sends the user a short digest that starts with rows graded and cells running. A script watcher is only a helper.
- **A delegate's "healthy" covers only its own arm.** Check throughput across every arm yourself; never infer it from silence.
- **After an agent dies** (rate limit, context handoff), check at once what it was driving or investigating, and relaunch the unfinished work.
- **One orchestrator owns each experiment's** driver, lanes and host list. To change another experiment's placement, message its owning thread; never touch its lanes.

## 6. Fix

- **Fix the cause, not the symptom.** For every issue a run or a check finds, ask why it happened in the first place, and keep asking until you reach the mechanism. Then ask what change would make the whole class impossible or caught early, and make that change.
  - Examples of a principled fix: a test, a preflight check, a guard in the harness, or a corrected design.
  - A restart, a requeue or a rebuild is not a fix.
  - A hotfix is allowed only to keep rows flowing while the principled fix lands. Record both, and never close an issue on the hotfix.
  - Check whether the same class can occur in other arms, apps or hosts, and fix it there too.
- **Hand the why-chain to whoever fixes it.** When a checker or delegate reports an issue, the fixer gets the why-chain along with it and returns the principled change.
- Validate a fix by replaying recorded incidents on the branch, in minutes, before rebuilding lanes; use a lane rerun only to confirm.
- Cut every worktree from `origin/main`, not from the shared checkout's branch.
- Cap concurrent agent builds and tests per host (for example `GOFLAGS=-p=4`).

## 7. Finish and clean up

- Tear down your own idle clusters when a round ends; idle leftovers still hold memory and processes.
- **Never move platforms mid-campaign** (for example kind to a managed cloud cluster). It splits results by platform, and the harness may not support it.
- **Cloud resources:**
  - tag everything, keep a manifest and a teardown script, and set a budget alarm;
  - a managed control plane still bills after its nodes scale to zero.

## Reference timings seen

Calibrate minimums with these and update them when you measure better ones.

| stage | seen |
| --- | --- |
| kind lane up, SREGym app | 5-7 min |
| GitLab lane, fresh / reused app / warm pool | 20-25 / about 11 / 1-2 min setup |
| recommend-only cloud agent, injection to plan | about 4 min |
| SDO SREGym row, warm lane, end to end | 25-35 min (much of it gates and settle, not agent work) |
