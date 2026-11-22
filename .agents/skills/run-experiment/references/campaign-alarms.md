# Campaign alarms, scheduling rules, driver discipline and timings

Thresholds and habits that turn the in-principle minimum into checks. Each alarm is a watcher check that flags the arm and names an owner, never just a note.

## Alarm per stage

These tighten the general rule that actual above 2× the minimum is a bug.

| stage | alarm |
| --- | --- |
| one-off setup | longer than one problem's minimum |
| claim, reset, gate with a warm pool | above about 2 min |
| fresh lane build | above 2× its reference timing |
| agent-active share of a run | below about 25% of wall time |
| dependency chain | above 1.5× its minimum |
| lane utilization with work queued | below about 70% |

## Scheduling rules

Each rule ends with the principled fix, so the class cannot recur.

- **Start every serial prerequisite at launch, in parallel.** A second app's baseline or pool build that waits for the first app's runs leaves its lanes idle. Fix: the plan lists every prerequisite with its minimum, and the driver starts them all at launch, within the build limits below; a prerequisite not started by launch is a plan defect.
- **Give pool builds and run builds separate, load-gated limits.** One shared `max_builds` lets pool builds starve run builds, and the reverse. Fix: set each limit separately, gate each on its host's load threshold (kept in the host state file), and have the watcher report every idle lane's blocking reason (build limit, load gate, disk floor, no ready pool lane).
- **Calibrate `min_free` to footprint.** Measure one lane's disk footprint first. Fix: the preflight sets `min_free` to footprint × planned lanes plus the host's reserved headroom and refuses a smaller value.
- **Settle must not wait on pre-inject readiness** (for example a store baseline's promotion). A wait that answers idle once and then restarts never gives the two consecutive idle answers settle needs, so post-fault runs hit the cap and turn invalid. Fix: after the inject, settle treats a readiness-only answer as idle; a test covers the restart case.
- **Compare image IDs, not tags, across every lane host.** A mutable tag can point at different builds on different hosts. Fix: preflight compares IDs per host and deploys by immutable tag or digest.
- **An app without request metrics needs no telemetry decision.** The request-metrics detector reports an operation whose entry service has no `entries` record as not applicable (an `O3.detector-coverage` note), so a provided plan passes onboarding with the metrics source on; use one telemetry setting across arms. If `O4.detectors` still fails on `request-metrics.not-instrumented`, the map declares an `entries` record the app does not serve: fix the map, not the profile.
- **Gate run scheduling on a golden pool lane, with no fresh-build path.** Claimants added together with their pool fall back to fresh builds (26-30 min instead of a 1-2 min claim), and a claimant already holding a lane skips any external gate (a pass 2 paid 20 min after the gate was live). Fix: a runner `claim_only` mode in which a claimant never builds fresh and waits for a pool lane; the pool builds one golden lane per host first; the watcher flags any run on a fresh build.
- **Bound a skipped check.** A check skipped on a ledger-proven lane still took 4 min (it waited on the operator's idle answer). Fix: a ledger-proven lane goes swap to run in under a minute, the skipped check's remaining wait is bounded (about 30 s), and a test covers the swap-to-run path.
- **Give each pool its own build slots.** Pool builds share `max_builds` with each other and with run builds, so the first-sorted pool starves the second (hotel waited behind three GitLab pool builds). Fix: separate, load-gated limits per pool and for run builds; the watcher names the blocking limit for every idle lane.
- **Report claimants waiting per pool in every digest.** Idle lanes with queued claimants are invisible otherwise. Fix: a status line "claimants waiting for pool P: N, ready lanes: M" per pool, and an alarm when N > 0 while a lane is free for 10 minutes.
- **Read the grader's reason before rerunning an invalid cell.** A validity check can be an artifact of the operator under test: fuzzops' `hurts` stops at the operator's first action, so an operator that detected in 16 s made a harmful fault read `no_harm`. Fix: the grader judges at least the fault's admitted onset span or reports `undetermined`; until then record the cell with its evidence instead of rerunning.
- **Bind each hold pool only to its own hosts.** The first-sorted pool takes every lane it may use, even ones meant for another app. Fix: list the allowed hosts per pool in the spec, and have the preflight refuse a lane assigned to a pool whose host list excludes it.

- **Read a row's `valid` together with its `evidence_incomplete` stamp, and a failure's first cause, not its prefix.** `not injected: X` is every failure before the injection (here: a missing credentials Secret, a node agent, an SDO pod that stayed Pending), so a probe read as "the selector fault is not injected" while the fault was never reached; and a row with no verified evidence copy printed `NOT GRADED` yet the lane record called it valid. Fix: the lanes workload record is invalid when fastloop's evidence gate stamped the row, and the failure text names the pod's scheduling reason and events instead of a bare timeout.
- **A stub arm cannot judge a fault no restore-from-source undoes.** The scripted worker only restores what the accepted source describes, so a cluster-scoped fault (an admission webhook outage) ends with the oracle failing and, by design, the lane taken down as `fault still active`; that says nothing about the injector or about vcluster. Fix: validate such problems with a real agent arm and judge the stub row by injection and detection only.
- **A copy verified by size must not verify a file the copy rewrites.** `copy_interval` rewrites `index.jsonl` (Python spacing, only the copied units) while the Go sink writes it compact, so the size check refused every copy whose index differed in length. Fix: verify the rewritten index by its records.

## Driver discipline

- **Keep the driver resumable:** chaining logic in a script with state on disk, never in one agent turn. Write decisions and a status line (every 30 minutes) to `NOTES.md` in the run directory so a replacement can take over after a rate limit. Spread agents across accounts.
- **Wait by bounded foreground polling** (about every 5 minutes), never by ending a turn on a monitor.
- **Every harness wait has a deadline and logs progress.** A wait that ends silently (for example for a seeded controller Pod that never becomes ready) holds a lane for an hour while the driver counts it as running. Fix: bound the wait, end the row with a named error so the driver requeues it, and give the driver a stall rule that covers every wait.
- **A removed plan can still hold and bill a lane.** A lane `held` for incomplete evidence stays held after its plan is removed: an EKS lane billed 4 h 40 min that way, and the next plan sat queued behind it and was removed without running. Fix: after removing a plan, and after adding one, read `lanes status` within 10 minutes. Free every lane the removed plan holds through the manager: write `operator/evidence-waiver.json` and rebuild the manifest (`python -m benchmarks.fuzzops.cellevidence manifest RUN_DIR`), then `lane release`. A plan still unplaced after 10 minutes needs its blocking lane named.
- **Stop a plan's work with `lanes plan remove NAME`, never `lanes remove EXPERIMENT`.** Removing an experiment counts as a failed attempt, so the plan queues an `-x1` retry and starts a fresh lane build. `plan remove` deletes the plan's experiment directories, records included: copy any record you need first, or pass `--keep`.
- **Set the run cap from the run's own minimum, not a generous default.** Settle and the release's idle wait are bounded by the cap. An incident nothing will close (escalated, stub agent) cost one cap in settle and up to another in the release (2 h each at `cap: 120m`).
- **Count failures per problem, not per lane, and park on the second identical one.** A problem that fails the same way on every lane is requeued forever when only lane failures are counted (one failed 17 times at 30 minutes each).

## Reference timings seen

Calibrate minimums with these and update them when you measure better ones.

| stage | seen |
| --- | --- |
| kind lane up, SREGym app | 5-7 min |
| GitLab lane, fresh / reused app / warm pool | 20-25 / about 11 / 1-2 min setup |
| GitLab on an EKS vcluster lane, fresh build | about 18 min |
| recommend-only cloud agent, injection to plan | about 4 min |
| SDO SREGym row, warm lane, end to end | 25-35 min (much of it gates and settle, not agent work) |
