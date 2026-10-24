---
name: run-experiment
description: Run live SDO experiments on SREGym with real LLM agents through fastloop, from frozen code and a warm lane to a per-incident report against a baseline, while keeping the feedback loop fast and unblocking it proactively. Use for every live SREGym run, smoke or full experiment; when rerunning or comparing a stream; when measuring playbook programs, the fast path or drift detection; or when a run is slow, stuck or blocked.
---

# Run an experiment

A live run is the top tier of [test-sdo](../test-sdo/SKILL.md): start one only after tiers 0 and 1 pass on the same commit. Fastloop is the only runner unless the user opts out. Commands and flags live in [the fastloop README](../../../benchmarks/sregym/experiments/fastloop/README.md); this skill gives the order and the checks.

Before step 1, read the machine's host state (`~/.agent-state/hosts/<hostname>.md` when it exists): lanes, cluster prefix, image builder, seed, credentials keeper and cache environment.

## Approval

Live runs spend real money and quota.

1. The first time, ask the user which live runs are pre-approved, before one is near. Recommend: small smokes on the cheapest approved model are pre-approved; each full experiment is approved separately.
2. Save the answer in your persistent agent state (for example `~/.agent-state/run-experiment/live-approval.md`) and reuse it; update it as soon as the user changes it.
3. A live run outside the saved preference waits for explicit approval. Keep running the cheaper tiers meanwhile.

## Procedure

1. **Preflight first.** Run the host and lane preflight before any build. It takes seconds and catches host limits, quota and image faults that would otherwise surface after an hour of setup.
2. **Freeze the code.** Create a detached worktree at the commit to test, initialize `third_party/sregym` from inside it, and confirm the submodule's own `git rev-parse --show-toplevel`. A local-only SREGym commit is fetched from another checkout with `uploadpack.allowAnySHA1InWant`.
3. **Gate.** Run tiers 0 and 1 in that worktree. Stop on a real failure.
4. **Build images only when needed** (see step 5), once, with `scripts/build_sdo_images.sh` and a tag unique to the frozen commit, without the scripted-worker switch. Run the image self-checks, then write a ready marker that other lanes can wait on.
5. **Reuse a warm lane.** Rebuild and reload images only when image-baked content changed.
   - Lane created with `--dev-mount`: run `fastloop reset --dev-mount` and skip the build; the mount carries Python and the host-built operator.
   - No such lane: delete only your own lane's cluster, then run `fastloop up --dev-mount` with the image tag and a populated `--sregym-dir`; grading needs the applications checkout.
6. **Credentials.** Start the keeper for the lane and confirm the agent credentials Secret exists before `run`.
7. **Run the stream** through the [detached launcher](../../../docs/testing-guide.md#detached-launches), wrapping `fastloop run` and reusing the lane record to adopt existing work. Add `--continue-on-error`, plus `--detection-timeout` for streams with latent faults.
8. **Check early.** After the first incident of each kind, read its row and the controller log. On a plumbing failure, stop the run, save the evidence, and go back to "When something fails" in [test-sdo](../test-sdo/SKILL.md).
9. **Report** per incident against the baseline run of the same stream. The fields and their sources are in [report.md](references/report.md).
10. **Clean up.** Kill only the processes you started, by exact PID; leave the cluster up when a rerun is planned.

## Keep the loop fast

The experiment is the incidents; everything else is overhead to remove.

1. Before launching, write the expected duration of each phase (preflight, build, lane, gate, each incident). Record actual start and end times in a timings file in the run directory as you go.
2. When a phase takes more than twice its estimate, or setup outlasts the incidents, stop waiting. Find the slow step from its logs, fix it, and add a guard or test so it cannot recur. Do not just note it in the report.
3. Prefer removing a step to speeding it up: a warm lane beats a faster image load, and a dev mount beats both.
4. Report each slow-loop fix with its before and after times.

## Unblock proactively

- Do not wait on the user for operational choices. Pick the least destructive fix scoped to your own resources, record it, and continue.
- A blocker found late is a check that ran too late: move it earlier (into preflight or a lower tier) when you fix it.
- If a delegated agent repeats the same failure twice, find the root cause yourself and hand it the exact fix.
- Relaying status while a run is stuck is still blocking. Fix it, then report.

## Rules

- Use only your own lanes; never touch other users' clusters or processes.
- Stop a run as soon as it is known to run stale or broken code. Fix, refreeze, rebuild.
- Load one image set per lane, once. Scripted gates and live slots share it: switch agents with `fastloop reset` and run flags, never with a second image set.
- Use `--time-scale 0.1` for smoke runs so timer waits do not dominate; leave it at 1 for measured experiments.
- One build serves every lane that runs the same commit; a lane that needs a fix runs from its own frozen worktree and tag.
- Each incident row must explain itself. A missing skip reason, verdict or learning decision is a bug to fix, not a gap to explain in the report.
- Never print, copy or commit credentials. After a run, check the results tree has no provider auth files.
- Report times in the user's time zone.

Read [references/failure-modes.md](references/failure-modes.md) before step 8, and whenever a run behaves unexpectedly.
