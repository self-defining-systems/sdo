# Campaign alarms, driver discipline and timings

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

## Driver discipline

- **Keep the driver resumable:** chaining logic in a script with state on disk, never in one agent turn. Write decisions and a status line (every 30 minutes) to `NOTES.md` in the run directory so a replacement can take over after a rate limit. Spread agents across accounts.
- **Wait by bounded foreground polling** (about every 5 minutes), never by ending a turn on a monitor.
- **Every harness wait has a deadline and logs progress.** A wait that ends silently (for example for a seeded controller Pod that never becomes ready) holds a lane for an hour while the driver counts it as running. Fix: bound the wait, end the row with a named error so the driver requeues it, and give the driver a stall rule that covers every wait.
- **Count failures per problem, not per lane, and park on the second identical one.** A problem that fails the same way on every lane is requeued forever when only lane failures are counted (one failed 17 times at 30 minutes each).

## Reference timings seen

Calibrate minimums with these and update them when you measure better ones.

| stage | seen |
| --- | --- |
| kind lane up, SREGym app | 5-7 min |
| GitLab lane, fresh / reused app / warm pool | 20-25 / about 11 / 1-2 min setup |
| recommend-only cloud agent, injection to plan | about 4 min |
| SDO SREGym row, warm lane, end to end | 25-35 min (much of it gates and settle, not agent work) |
