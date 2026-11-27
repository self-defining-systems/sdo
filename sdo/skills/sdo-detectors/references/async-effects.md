# Asynchronous effects

- A read in `Setup` or `Steps` can return `NotYet` to retry within the iteration deadline. Completed setup steps
  are reused. The wait is excluded from step latency and recorded as the step's convergence.
- Background work users wait for: find each async path in source, config and live traffic (workers, consumers,
  schedules, the queues they read, the user actions that enqueue to them). For each user action that hands work
  off, write one journey: perform it, then read its effect in a step wrapped in `traffic.Eventually(read)`. The
  wrapper retries any rejected answer below 500 until the iteration deadline. Set the workload's
  `iterationTimeout` a few times above the healthy delay (at most 30 s). Judge sustained lateness, not one late
  sample: a longer `window` with a matching `maxErrorRate` and `maxAge` on that scenario. Keep its rate low and
  its cleanup free of new queued work. Re-derive these paths in the health refresh after an upgrade: releases add
  job types and background migrations.
- Queue health is a clue, never a user test: the oldest job's age per queue (better than depth), arrival against
  completion rate, retries and dead jobs, worker saturation, from the metrics the application exports, judged over
  a sustained window against the healthy range.
- The iteration's start time is in `State[StateNow]`; format dates from it instead of a clock or a far-future constant.

