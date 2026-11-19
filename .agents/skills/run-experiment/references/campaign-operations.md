# Running a multi-lane campaign

Rules for keeping a campaign of many lanes, hosts and arms producing graded rows. Each rule comes from a mistake that cost hours; most were repeated because the lesson lived in one account's memory. Keep host facts in the host state file, not here.

## Maintain this file

- When a campaign mistake costs time, or the user corrects how experiments are run, add or sharpen a rule here in the same turn and land it on main. Account-local memory is not enough: other accounts, threads and subagents read the skill, not your memory.
- One rule per mistake class, stated as what to do. Merge with an existing rule rather than adding a near-duplicate.

## Oversight

- **Watch from the first minute, not after a stall.** When a run starts, schedule a recurring check every 20 minutes. Each check sends a subagent through a written checklist kept next to the driver:
  - graded rows in the last window against the window before;
  - every failure, with its cause read from the cell's own logs;
  - cells stuck far past their usual time, and idle lanes while work is queued;
  - setup time per run against the warm-start target;
  - host load, memory pressure, disk, and whether drivers are alive.

  The orchestrator acts on the findings and sends the user a short digest. A script watcher is only a helper: it never replaces the agent check.
- **A delegate's "healthy" covers only its own arm.** Check graded-row throughput across every arm yourself; never infer it from silence.
- **After an agent dies** (rate limit, context handoff), check at once what it was driving or investigating. Unfinished investigations are lost: relaunch them and say so.

## Drivers

- Start every long-lived driver, feeder or loop with `systemd-run --user --unit=<name> --collect -p Restart=always -p RestartSec=30 -E PATH=$PATH -E HOME=$HOME --working-directory=<dir> -p StandardOutput=append:<log> <cmd>`.
  - Processes started from an agent session live in that session's cgroup, and `setsid nohup` does not escape it: they die when the session is recycled.
  - A user unit's default `PATH` lacks `kubectl`, `uv`, `go` and `aws`.
- Restart with `systemctl --user restart <name>`, never kill and relaunch by hand.
- One orchestrator owns each experiment's driver, lanes and host list. To change another experiment's placement, message its owning thread; never edit its host list or touch its lanes.

## Throughput

- Every progress report starts with rows graded since the last report and cells running, mapped to the questions.
- A fix blocks only the arms that depend on it. Keep independent arms (baselines, other apps, other passes) running.
- **Queue baselines independently.** If baseline cells are queued only as twins of treatment cells, a treatment stall silently stalls the baseline too.
- **Stop a failure loop early.** The same failure on two or more lanes or hosts is a pipeline bug, not lane flakiness.
  - Park that arm after the second repeat, and find the root cause on recorded evidence.
  - Automatic lane rebuilds on repeated pre-injection failures only burn lane time when the cause is in the pipeline.
- **Change one layer at a time.** Never swap runner, SDO build, memories, lanes and dispatcher at once. Keep the last working pipeline producing rows while a new one is smoked with several parallel cells, watched live.
- **Start from an installed baseline or a warm pool**, never a fresh install per lane, unless install is the question. Measure setup per run and treat anything above the warm-start target as a bug.
- Keep rounds small (one wave, first results in one to three hours). Align questions, arms, metrics and protocol with the user before the first run.

## Hosts

- **Read a host before placing work on it:**
  - load per core;
  - threads in disk sleep (`ps -eLo stat | grep -c ^D`);
  - memory pressure and `MemAvailable`;
  - free local disk;
  - the other experiments' clusters already there.

  A host with hundreds of threads in disk sleep is stalled even if its CPU looks free.
- **Move off a failing host within about 15 minutes**, and delegate the host repair. Do not debug hosts in the orchestrating thread.
- **Separate experiments that share a host in both directions.** Give each its own cpuset or slice and a load gate. One side's cpuset does not keep the other side's unpinned lanes off its cores.
- **Stagger lane creation:** at most about three image imports at a time per host.
- **Disk:** keep lanes, images and evidence on local disk, never NFS. Keep a disk watcher on every host.
- **Clean up idle clusters.** Tear down your own idle clusters when a round ends; idle leftovers from earlier rounds still hold memory and processes.
- **Cap concurrent agent builds and tests per host** (for example `GOFLAGS=-p=4`), and check load before fanning out.

## Fix loop

- Validate a fix by replaying recorded incidents on the branch before rebuilding lanes; use a lane rerun only to confirm.
- Cut every worktree from `origin/main`, not from the shared checkout's branch.

## Platform and cost

- Do not move a campaign to a different platform mid-run (for example kind to a managed cloud cluster). It splits results by platform, and the harness may not support it. Check the lane backend before proposing it.
- Cloud resources:
  - tag everything, keep a manifest and a teardown script, and set a budget alarm;
  - a managed control plane still bills after its nodes scale to zero.
