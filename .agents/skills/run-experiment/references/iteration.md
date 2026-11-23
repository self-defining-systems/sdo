# Bringing up a new eval path and iterating fast

Read this when a path (a benchmark, an app pack, a cluster kind, a lane pool) has never run live, or when a debug loop feels slow. Stage-by-stage rules for a running campaign are in [campaign-operations.md](campaign-operations.md); alarms and driver discipline are in [campaign-alarms.md](campaign-alarms.md). This file adds only what those do not say. Each rule is the practice, then its reason.

## 1. Inner loop first

- **Measure time per debug iteration (change → verdict on the failing item) as the first KPI.** Reason: a loop that takes an hour per attempt turns ten bugs into a day.
- **More than about 30 minutes per attempt is a defect in the loop; fix the loop before the next fix.**
- **Keep a warm debug lane at a known state.** Apply a branch fix straight into it (image swap, dev mount, or an image push and a restart) and rerun only the failing row or series position. Reason: a rebuilt lane, a re-pin or a full plan pays setup minutes for seconds of change.
- **Never debug through fix → merge → new pin → new pool → full rerun.** Merge in the background; the full plan is only the final confirmation on the merged pin.

## 2. Find bugs in batches, not one per live attempt

- **Before going live, sweep the whole path statically.** Walk every step against the known failure classes in [campaign-alarms.md](campaign-alarms.md) (step environment, waits without deadlines, node-local paths, host-global names). Reason: a live attempt finds one bug and costs a lane build; a sweep finds a class.
- **Check inputs offline:** render the manifests, dry-run the apply, confirm every image and tag exists.
- **Rehearse for free first:** a local kind or vcluster host with a stub agent, through the same manager and adapters as the live plan ([campaign-alarms.md](campaign-alarms.md)).
- **Run every item in one plan with keep-going.** Reason: a round then yields every distinct failure instead of the first.
- **Keep adapter conformance tests in CI.** Every adapter verb and action runs against the environment the manager gives it; a new verb that breaks the contract fails before a lane exists.
- **Reproduce a logic bug offline against recorded evidence** (`fastloop replay`, saved rows, saved logs) and prove the fix there. A lane rerun only confirms.

## 3. Speed from first principles

- **The metric is per-row latency against a first-principles floor,** and the floor holds only the experiment's own work: inject, detect, agent or playbook, verify, plus learning only on the arm that uses it. Reason: a floor padded with overhead makes waste look like a ratio near 1.
- **Never count a reinstall, learning nobody consumes, or a fixed window as "minimum".** Recompute the ratio from the raw row stages and state the floor's parts beside it; a driver's own ratio is a claim until an auditor recomputes it (section 6).
- **Parallelism is a capacity knob, not a speed-up.** Report per-row latency separately from rows per hour; more lanes leave latency unchanged.
- **When actual is several times the floor, restructure before trimming.** In order of payoff:
  1. reuse or restore a resident install instead of reinstalling;
  2. pre-warm before the clock starts;
  3. move non-critical work off the critical path (evidence capture, background reset);
  4. replace timers with event-driven waits;
  5. skip work whose result nothing reads (learning on a control arm).
- **A fast path that is off, or falls back every time, is not a speed-up.** Report, per row, whether it was used and the reason when it was not. A reuse path with zero live successes saves nothing, whatever merged.
- **Check a speed claim's comparison.** An after-number on one platform against a before-number on another, or over fewer positions, proves nothing.

## 4. Speed changes have side effects

- **Moving work earlier or running it continuously can break the app under test.** Probes during a rollout hit a per-user rate limit; a warm lane's in-memory trace store grew while idle. Reason: the app's own limits were sized for the slow path.
- **After every speed change, rerun a known-good item and compare verdicts and row fields with its earlier run.** A faster wrong verdict is a regression.
- **Give a shortened wait a fallback bound,** not only a shorter one: the event may never come.

## 5. The control plane needs a healthy host

- **The machine that drives remote clusters (manager, steps, kubectl, builds) must not be a shared, overloaded host.** Reason: its slow API calls time out and look like lost lanes, rows and series positions.
- **Gate every manager start and step on that machine's load per core,** and read it before blaming a lane ([campaign-operations.md](campaign-operations.md) section 2). Prefer a dedicated host, or one inside the cloud account, close to the clusters.
- **Retry a timed-out cluster call, and record it as a harness failure,** never as a lost lane.

## 6. Agents

- **Rotate long-running drivers every few hours.** Long contexts drift: inflated minimums, stale budgets, wrong timestamps, ignored rules. Start the replacement from the log and `NOTES.md`.
- **Verify claims with a fresh, independent auditor that reads code and records,** not the driver's summaries. Give it the claim, not your conclusion; ask it to recompute every ratio from raw rows.
- **Never leave lanes billing without a live driver.** On a driver's death, take over or release lanes through the manager at once ([campaign-alarms.md](campaign-alarms.md)).
- **Spread agents across accounts and plan for rate limits,** with all state resumable from the log.
- **Exactly one driver per effort.** Check for a live one before taking over ([campaign-alarms.md](campaign-alarms.md)).

## 7. Shared infrastructure

- **Every CLI verb rejects flags it does not support, and `--dry-run` never acts.** Reason: a shared flag parser silently ignoring a flag turns a check into a launch. Test this per verb.
- **Validate a host's lane count against its capacity limits when the plan loads,** not through failed builds.
- **Never restart a shared manager while another plan runs.** Restart at a quiet point agreed with every plan's owner; a restart drops in-flight steps.
- **Put private rehearsals on a private root,** so they cannot take shared lanes or a shared ledger.
- **Scale out only after one lane builds clean.** Reason: a build bug multiplies by the lane count.

## 8. Money

- **Give each run a ledger cap that follows the run across hosts and managers.** A fresh ledger on a new manager is blind to earlier spend; carry the spent total and keep an account-level cap beside it.
- **Set an idle-lane alarm and release lanes through the manager.** Check it after every plan change.
- **Size warm pools to the plan.** A pre-warmed lane bills while idle; it pays only when the rebuild it avoids costs more than its idle hours.
- **Spend on the stated goal.** A build-out for parallelism when the goal was latency billed lanes that never ran a row.
