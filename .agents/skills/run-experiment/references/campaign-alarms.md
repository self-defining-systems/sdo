# Campaign alarms, scheduling rules and timings

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

- **Start every serial prerequisite at launch, in parallel.** A second app's baseline or pool build that waits for the first app's runs leaves its lanes idle for hours. Fix: the plan lists every prerequisite with its minimum, and the driver starts them all at once; a prerequisite not started by launch is a plan defect.
- **Give pool builds and run builds separate, load-gated limits.** One shared `max_builds` let pool builds starve run builds (and the reverse) while hosts sat idle. Fix: set each limit separately, gate each on host load, and have the watcher report every idle lane's blocking reason (limit, load gate, disk floor, no golden lane).
- **Calibrate `min_free` to footprint.** Measure one lane's disk footprint first; set `min_free` to footprint × planned lanes plus the floor. Fix: the preflight computes it and refuses a smaller value.
- **Settle must not wait on pre-inject readiness** (for example a store baseline's promotion). A wait that answers idle once and then restarts never gives the two consecutive idle answers settle needs, so post-fault runs hit the cap and turn invalid. Fix: after the inject, treat a readiness-only answer as idle, with a test.
- **Compare image IDs, not tags, across every lane host.** A mutable tag can point at different builds on different hosts. Fix: preflight compares IDs per host and deploys by immutable tag or digest.
- **Decide the no-request-metrics path before scheduling an app.** An app without request metrics cannot pass onboarding with a provided plan while the metrics source is on: the request-metrics floor detector reports "not instrumented" as firing. Fix: pick the deployer path or the telemetry profile in the plan, and use the same setting in every arm.
- **Bind each hold pool only to its own hosts.** The first-sorted pool takes every lane it may use, even ones meant for another app. Fix: list the allowed hosts per pool in the spec.

## Driver discipline

- **Keep the driver resumable:** chaining logic in a script with state on disk, never in one agent turn. Write decisions and a status line (every 30 minutes) to `NOTES.md` in the run directory so a replacement can take over after a rate limit. Spread agents across accounts.
- **Wait by bounded foreground polling** (about every 5 minutes), never by ending a turn on a monitor.

## Reference timings seen

Calibrate minimums with these and update them when you measure better ones.

| stage | seen |
| --- | --- |
| kind lane up, SREGym app | 5-7 min |
| GitLab lane, fresh / reused app / warm pool | 20-25 / about 11 / 1-2 min setup |
| recommend-only cloud agent, injection to plan | about 4 min |
| SDO SREGym row, warm lane, end to end | 25-35 min (much of it gates and settle, not agent work) |
