# Bringing up a new eval path and iterating fast

Read this when a path (a benchmark, an app pack, a cluster kind, a lane pool) has never run live, or when a debug loop feels slow. Stage-by-stage rules for a running campaign are in [campaign-operations.md](campaign-operations.md); alarms and driver discipline are in [campaign-alarms.md](campaign-alarms.md). This file adds only what those do not say. Each rule is the practice, then its reason.

## 1. Inner loop first

- **Measure time per debug iteration (change → verdict on the failing item) as the first KPI.** Reason: a loop that takes an hour per attempt turns ten bugs into a day.
- **More than about 30 minutes per attempt is a defect in the loop; fix the loop before the next fix.**
- **Keep a warm debug lane at a known state.** Apply a branch fix straight into it (image swap, dev mount, or an image push and a restart) and rerun only the failing row or series position. Reason: a rebuilt lane, a re-pin or a full plan pays setup minutes for seconds of change.
- **Never debug through fix → merge → new pin → new pool → full rerun.** Reason: each hop adds a lane build and a merge wait to a one-line change. Merge in the background; the full plan is only the final confirmation, pinned to merged commits ([campaign-alarms.md](campaign-alarms.md)); a debug lane may run unmerged branch code, but report nothing from it.

## 2. Find bugs in batches, not one per live attempt

- **Before going live, sweep the whole path statically.** Walk every step against the known failure classes in [campaign-alarms.md](campaign-alarms.md) (step environment, waits without deadlines, node-local paths, host-global names). Reason: a live attempt finds one bug and costs a lane build; a sweep finds a class.
- **Check inputs offline:** render the manifests, dry-run the apply, confirm every image and tag exists. Reason: these failures are deterministic and each costs a lane build.
- **Rehearse for free first** on a local kind or vcluster host with a stub agent ([campaign-alarms.md](campaign-alarms.md)).
- **Run every item in one plan with keep-going.** Reason: a round then yields every distinct failure instead of the first.
- **Keep adapter conformance tests in CI** ([campaign-alarms.md](campaign-alarms.md), "conformance first"). Reason: a verb that breaks the contract then fails before a lane exists.
- **Reproduce a logic bug offline against recorded evidence** (`fastloop replay`, saved rows, saved logs) and prove the fix there. A lane rerun only confirms.

## 3. Speed from first principles

- **Measure per-row latency against a first-principles floor,** and let the floor hold only the experiment's own work: inject, detect, agent or playbook, verify, plus learning only on the arm that uses it. Reason: a floor padded with overhead makes waste look like a ratio near 1.
- **Never count a reinstall, learning nobody consumes, or any wait beyond the oracle's own verification window as "minimum".** Recompute the ratio from the raw row stages and state the floor's parts beside it; a driver's own ratio is a claim until an auditor recomputes it (section 6 below).
- **Treat parallelism as a capacity knob, not a speed-up.** Report per-row latency separately from rows per hour; more lanes should leave latency flat, and if it rises a shared resource is saturated ([campaign-operations.md](campaign-operations.md) section 4).
- **When actual is several times the floor, restructure before trimming.** In order of payoff:
  1. reuse or restore a resident install instead of reinstalling ([campaign-operations.md](campaign-operations.md) section 2);
  2. pre-warm before the clock starts;
  3. move non-critical work off the critical path (evidence capture, background reset);
  4. replace timers with event-driven waits;
  5. skip work whose result nothing reads (learning on a control arm).
- **Report per row whether a fast path ran and why not.** A path that is off or always falls back is not a speed-up, however much code supports it.
- **Check a speed claim's comparison.** An after-number on one platform against a before-number on another, or over fewer positions, proves nothing.

## 4. Speed changes have side effects

- **Re-check the app's limits when moving work earlier or running it continuously.** Reason: they were sized for the slow path (per-user rate limits hit by probes during a rollout, in-memory stores that grow while a warm lane idles).
- **After every speed change, rerun a known-good item and compare verdicts and row fields with its earlier run.** A faster wrong verdict is a regression.
- **Give an event-driven wait a deadline,** not only a shorter wait: the event may never come.

## 5. The control plane needs a healthy host

- **Run the machine that drives remote clusters (manager, steps, kubectl, builds) on a dedicated, unloaded host,** or one inside the cloud account. Reason: on a shared, overloaded host its API calls time out and look like lost lanes, rows and series positions.
- **Gate manager starts and steps on that machine's load per core,** and read it before blaming a lane ([campaign-operations.md](campaign-operations.md) section 2).
- **Retry a timed-out cluster call; if it still fails, record a harness failure,** never a lost lane.

## 6. Agents

- **Rotate a long-running driver at the first sign of drift or every 3-4 hours.** Long contexts drift: inflated minimums, stale budgets, wrong timestamps, ignored rules. Start the replacement from the log and `NOTES.md`.
- **Verify claims with a fresh, independent auditor that reads code and records,** not the driver's summaries. Give it the claim, not your conclusion; ask it to recompute every ratio from raw rows.
- **Never leave lanes billing without a live driver.** Reason: nobody else will release them. On a driver's death, take over or release lanes through the manager at once ([campaign-alarms.md](campaign-alarms.md)).
- **Spread agents across accounts and plan for rate limits** ([campaign-alarms.md](campaign-alarms.md), driver discipline).
- **Exactly one driver per effort:** check for a live one before taking over ([campaign-alarms.md](campaign-alarms.md)).

## 7. Shared infrastructure

- **Every CLI verb rejects flags it does not support, and `--dry-run` never acts.** Reason: a shared flag parser that ignores a flag turns a check into a launch; test it per verb ([campaign-alarms.md](campaign-alarms.md)).
- **Validate a host's lane count against its capacity limits when the plan loads,** not through failed builds. Reason: a failed build reports the limit late and unclearly.
- **Never restart a shared manager while another plan runs.** Restart at a quiet point agreed with every plan's owner; a restart can drop in-flight steps.
- **Put private rehearsals on a private root,** so they cannot take shared lanes or a shared ledger.
- **Scale out only after one lane builds clean.** Reason: a build bug multiplies by the lane count.

## 8. Money

- **Give each run a ledger cap that follows the run across hosts and managers.** A fresh ledger on a new manager is blind to earlier spend; carry the spent total and keep an account-level cap beside it.
- **Set an idle-lane alarm and release lanes through the manager** ([campaign-alarms.md](campaign-alarms.md)); read lane status after every plan change.
- **Size warm pools to the plan.** A pre-warmed lane bills while idle; it pays only when the rebuild it avoids costs more than its idle hours.
- **Spend on the stated goal.** Parallelism capacity does not serve a latency goal, and its idle lanes bill without running rows.
