---
name: iterate-sdo
description: Choose the cheapest SDO test tier that reproduces a bug or validates a change, and climb the tier ladder before any live LLM run. Use when debugging SDO controller, broker, memory-edge, responder, reflection, drift or fast-path behavior, when a live run fails, or when an iteration loop is taking too long.
---

# Iterate on SDO

A live LLM run costs tens of minutes and millions of tokens, while the hermetic gate takes seconds. Find each bug at the lowest tier that can show it, and climb to a live run only when every tier below it passes.

## Tier ladder

| Tier | What it exercises | Command | Cost |
| --- | --- | --- | --- |
| 0 | Contracts, receipts, rows, CLI | `uv run --extra test pytest -q tests/unit/benchmarks/sregym` plus the focused Go module (`(cd sdo/operator/runtime && go test ./...)`) | 30–60 s |
| 1 | Real Go controller, broker, memory edge and builder against a fake Kubernetes API | `bash scripts/test_fastloop_hermetic.sh` | about 15 s |
| 2 | kind cluster with scripted agents and mounted source | `fastloop up --dev-mount`, then `fastloop reset` between runs | reset about 7 s, run a few minutes |
| 3 | One recorded stage re-executed offline | `fastloop replay --stage responder\|reflection\|broker` | seconds |
| 4 | Live models on kind | [run-fastloop-experiment](../run-fastloop-experiment/SKILL.md) | 30–120 min |

The exact flags for tiers 0–1 are in [the testing guide](../../../docs/testing-guide.md), and for tiers 2–3 in [the fastloop README](../../../benchmarks/sregym/experiments/fastloop/README.md). Use those commands; do not restate them.

## When something fails

1. Stop the slow loop. Do not rerun a whole live stream to debug one incident.
2. Save the evidence: controller logs, outcome documents under `.sdo/outcomes/`, result rows, worker results and model I/O. Never copy credentials.
3. Name the root cause with `file:line` before changing code.
4. Write the cheapest failing test that reproduces it, at Tier 0 or 1 when possible. Build it from the live evidence shape, for example a fixture under `sdo/operator/runtime/testdata/`.
5. Fix, run the tiers from the bottom up, then retry the failing tier.
6. A bug a lower tier missed means a test gap: add the test before climbing again.

## Rules

- Before a live run, gates 0 and 1 must pass on the exact commit the images are built from.
- Run live work from a frozen, detached worktree, so edits on the integration branch cannot change code under a running experiment.
- Before trusting a skip or a silent no-op, make it observable. A decision point that returns without a recorded reason (fast-path skip, drift deferral, learning failure) is a bug.
- Scripted agents prove plumbing, not learning: they return hand-written verdicts and programs. Claim that SDO learned something only from a live run.
- Make tests hermetic: no dependence on exported cluster prefixes, host paths or a warm cluster.
- When builds or tests are slow, check disk and network filesystem wait before CPU, and keep build caches on local disk.
- Leave no generated protobuf files deleted after a failed generation.
- Wait on a process with `pgrep -f "name[.]ext"`, and run the waiter as a script file. A shell whose own command line contains the literal `name.ext` matches itself and never exits.
