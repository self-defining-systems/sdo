# Onboarding fast gate

Judge a change to the probe SDK, onboarding skill or onboarding CLI in minutes, before a live onboarding run.

| Tier | Command | Needs | What it answers |
|---|---|---|---|
| Offline replay | `replay` once per recorded session, then `check --repo CHECKOUT` | bwrap, Go caches | Which rejection classes recorded agents hit still happen with the candidate, which disappeared, which are new; first-read bytes of the skill |
| Scripted e2e | `e2e --repo CHECKOUT --out DIR --scripted` | kind, docker, bwrap | The kit, bootstrap, `onboarding init`, live `journeys` and the export check work end to end, no model |
| Agent e2e | `e2e --repo CHECKOUT --out DIR [--model claude-haiku-5-5]` | the above and Claude credentials | Requests, weighted tokens, cost, rejections, journeys runs and wall time of one cheap real session ($1 cap) |

All commands are `uv run python -m benchmarks.onboarding_gate ...`; `--help` lists the flags.

- `replay` re-runs each recorded session's file edits and shell commands in a network-less sandbox, from its start
  commit, and snapshots the checkout before every validator call. Fidelity (agent commits rebuilt exactly, by tree) is
  printed per session. Results are cached in `--cache`.
- `check` judges the snapshots with each session's own kit (once, cached) and with the candidate's working tree, using
  `onboarding journeys --plan` and, for kits whose journeys does not include it, `memory check`. By default the
  candidate judges only the snapshots its recorded kit rejected and each session's final state; `--all` judges all.
  Findings that need the live app (`passes`, `latency-bound`, ...) cannot be replayed; the report lists the ones the
  agent saw.
- `e2e` runs the production launcher (`sdo/agents/onboarding/run_session.py`) against `synthetic/app`, a
  standard-library notes service (token sign-in, seeded reads, create/read/delete) on a local kind cluster
  `sdo-fastgate`, in an isolated root like a live session. `--scripted` swaps the model for `synthetic/scripted_agent.sh`,
  which writes the hand-written probes in `synthetic/fixture/`; keep that fixture valid when the SDK changes.

Keep caches and run directories outside the repository.
