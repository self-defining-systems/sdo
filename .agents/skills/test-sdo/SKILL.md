---
name: test-sdo
description: Test SDO changes and debug SDO behavior by climbing a tier ladder from unit tests through fastloop kind clusters to live LLM runs on SREGym, and report live runs against a baseline. Use when validating or debugging controller, broker, memory-edge, responder, reflection, drift or learned-mitigation behavior; when running, rerunning or comparing any live SREGym test or experiment; when a live run fails; or when an iteration loop is taking too long.
---

# Test SDO

A live LLM run costs tens of minutes and millions of tokens, while the hermetic gate takes seconds. Find each bug at the lowest tier that can show it, and climb to a live run only when every tier below it passes.

## Tier ladder

| Tier | What it exercises | Command | Cost |
| --- | --- | --- | --- |
| 0 | Contracts, receipts, rows, CLI | `uv run --extra test pytest -q tests/unit/benchmarks/sregym` plus the focused Go module (`(cd sdo/operator/runtime && go test ./...)`) | 30–60 s |
| 1 | Real Go controller, broker, memory edge and builder against a fake Kubernetes API | `bash scripts/test_fastloop_hermetic.sh` | about 15 s |
| 2a toy cluster | Three-service kind app, Calico, real controller/broker, scripted workers | `fastloop up --app toy --dev-mount` | registry delivery on first up; mounted source and warm reset thereafter |
| 2b | Hotel kind cluster with scripted agents and mounted source | `fastloop up --dev-mount`, then `fastloop reset --dev-mount` between runs | reset about 7 s, run a few minutes |
| 3 | One recorded stage re-executed offline | `fastloop replay --run-dir <results/run-id> --incident 0 --stage reflection` | seconds |
| 4 | Live models on kind | [run-experiment](../run-experiment/SKILL.md) | tens of minutes per incident |

The exact flags for tiers 0–1 are in [the testing guide](../../../docs/testing-guide.md), and for tiers 2–3 in [the fastloop README](../../../benchmarks/sregym/experiments/fastloop/README.md). Use those commands; do not restate them.

## Minimum tier by risk

Climb only as far as the change's risk requires, and only from green: run a tier after every cheaper tier that applies passes.

- Concurrency, restarts or failure handling: tier 1 plus Go `-race` tests with injected failures.
- Credentials or secrets: time-capped property tests with canaries in every excluded field, plus an independent read-only review.
- Classification, trust or scoring logic: tier 0 regression fixtures plus tier 3 replay.
- Platform wiring (images, permissions, mounts, config): tier 2.
- Prompts or model behavior: only tiers 3 and 4 show anything, so keep each such change small and isolated.

In a brief, state the change's risk classes, the tiers required before handoff, and which tier gates the next live run. A handoff that skips a required tier is not done. Live runs follow [run-experiment](../run-experiment/SKILL.md), including its approval policy.

## When something fails

1. Stop the slow loop. Do not rerun a whole live stream to debug one incident.
2. Save the evidence: controller logs, outcome documents under `.sdo/outcomes/`, result rows, worker results and model I/O. Never copy credentials.
3. Name the root cause with `file:line` before changing code.
4. Write the cheapest failing test that reproduces it, at Tier 0 or 1 when possible. Build it from the live evidence shape, for example a fixture under `sdo/operator/runtime/testdata/`.
5. Fix, run the tiers from the bottom up, then retry the failing tier.
6. A bug a lower tier missed means a test gap: add the test before climbing again. Record which tier caught each bug; if expensive tiers keep catching cheap bugs, make the lower tiers more representative instead of running the expensive ones more often.

## Rules

- Run every SREGym test, scripted or live, through fastloop unless the user opts out. Keep one warm `--dev-mount` lane and use `fastloop reset` between runs instead of rebuilding images or recreating clusters.
- Run `fastloop preflight` before image builds; up repeats host and image checks before any build or cluster change.
- Before a live run, gates 0 and 1 must pass on the exact commit the images are built from.
- Run live work from a frozen, detached worktree through the [detached launcher](../../../docs/testing-guide.md#detached-launches); adopt the shared lane record after an agent restart.
- Before trusting a skip or a silent no-op, make it observable. A decision point that returns without a recorded reason (coverage omission, drift deferral, learning failure) is a bug.
- Scripted agents prove plumbing, not learning: they return hand-written verdicts and programs. Claim that SDO learned something only from a live run.
- Make tests hermetic: no dependence on exported cluster prefixes, host paths or a warm cluster.
- When builds or tests are slow, check disk and network filesystem wait before CPU, and keep build caches on local disk.
- Leave no generated protobuf files deleted after a failed generation.
- Wait on a process with `pgrep -f "name[.]ext"`, and run the waiter as a script file. A shell whose own command line contains the literal `name.ext` matches itself and never exits.
