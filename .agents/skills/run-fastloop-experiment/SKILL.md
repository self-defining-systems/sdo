---
name: run-fastloop-experiment
description: Run a live SDO fastloop experiment on SREGym with real LLM agents, from frozen code through images, a lane cluster, an incident stream and a per-incident report against a baseline. Use when asked to run, rerun or compare a live fastloop stream, measure playbook programs, the trusted fast path or drift detection, or report on such a run.
---

# Run a fastloop experiment

A live run is the most expensive check SDO has, so it starts only after [iterate-sdo](../iterate-sdo/SKILL.md) tiers 0 and 1 pass on the same commit. Commands and flags live in [the fastloop README](../../../benchmarks/sregym/experiments/fastloop/README.md); this skill gives the order and the checks.

Before step 1, read the machine's host state (`~/.agent-state/hosts/<hostname>.md` when it exists). It names the lanes, cluster prefix, image builder, seed, credentials keeper and cache environment for this machine.

## Procedure

1. **Freeze the code.** Create a detached worktree at the commit to test, initialize `third_party/sregym` from inside it, and confirm the submodule's own `git rev-parse --show-toplevel`. A local-only SREGym commit is fetched from another checkout with `uploadpack.allowAnySHA1InWant`.
2. **Gate.** Run tiers 0 and 1 in that worktree. Stop on a real failure.
3. **Build images once** with `scripts/build_sdo_images.sh` and a tag unique to the frozen commit, without the scripted-worker switch. Run the image self-checks, then write a ready marker that other lanes can wait on.
4. **Bring up the lane.** Delete only your own lane's cluster, then run `fastloop up` with the new image tag and a populated `--sregym-dir`; grading needs the applications checkout.
5. **Credentials.** Start the keeper for the lane and confirm the agent credentials Secret exists before `run`.
6. **Run the stream** through the [detached launcher](../../../docs/testing-guide.md#detached-launches), wrapping `fastloop run` and reusing the lane record to adopt existing work. Add `--continue-on-error`, plus `--detection-timeout` for streams with latent faults.
7. **Check early.** After the first incident of each kind, read its row and the controller log. On a plumbing failure, stop the run, save the evidence, and go back to [iterate-sdo](../iterate-sdo/SKILL.md).
8. **Report** per incident against the baseline run of the same stream. The fields and their sources are in [references/report.md](references/report.md).
9. **Clean up.** Kill only the processes you started, by exact PID; leave the cluster up when a rerun is planned.

## Rules

- Use only your own lanes; never touch other users' clusters or processes.
- Stop a run as soon as it is known to run stale or broken code. Fix, refreeze, rebuild.
- One build serves every lane that runs the same commit; a lane that needs a fix runs from its own frozen worktree and tag.
- Each incident row must explain itself. A missing skip reason, verdict or learning decision is a bug to fix, not a gap to explain in the report.
- Never print, copy or commit credentials. After a run, check the results tree has no provider auth files.
- Report times in the user's time zone.

Read [references/failure-modes.md](references/failure-modes.md) before step 7, and whenever a run behaves unexpectedly.
