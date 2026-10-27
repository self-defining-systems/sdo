# SREGym benchmark boundary

All first-party SREGym integration code lives here. This package may depend on public production APIs under `sdo/`; production packages must never import this package or the external harness.

## Structure

- `adapter/` — translates SREGym execution into production SDO lifecycle/controller/responder APIs and persists benchmark transport evidence
- `protocol/` — benchmark conductor, HTTP, MCP submission, and strict-receipt evidence contracts
- `runner/` — experiment and pipeline configuration plus harness process orchestration
- `experiments/` — a few example configs (`sdo_example.toml`, `codex_baseline_example.toml`; copy and edit for your own runs) plus the assurance catalog and seeds (`assurance/composites.toml`, `assurance/seeds/`) that code reads at runtime. Incident-stream configs are generated with `uv run python -m benchmarks.sregym.runner.incident_stream --out-dir <dir>`, not checked in
- `analysis/` — benchmark result summarization
- `registry.yaml` — agent registry consumed by the benchmark runner
- `run.py` — benchmark experiment entry point

The external SREGym implementation is a separately pinned Git submodule at `third_party/sregym/`.

## Participants

- **sdo_codex** — the adapter-backed benchmark entry for the production SDO design
- **codex** — the memoryless stock Codex baseline, with its CLI version pinned in `registry.yaml`

## Boundary rules

- Put SREGym transport, conductor polling, submissions, receipts, experiment orchestration, and result analysis in this package.
- Put reusable production lifecycle, responder, contracts, and operational-memory behavior under `sdo/`.
- Keep benchmark verdicts, oracle answers, hidden fault identity, and injection metadata out of production requests, detectors, memory, and prompts.
- Keep changes to the external harness in `third_party/sregym/`; do not copy its implementation into first-party packages.
- The harness deploys exactly the image refs the app source declares. SDO accepts the source as desired state, so a harness-only tag is unexplained drift at time zero (a baseline incident before any injection) and the first reconcile would write the source's ref back. Source builds use a lane-private tag only to build and load; the harness then publishes the image on the lane's nodes under the declared ref (`source_deploy._declare_image`) and applies the unmodified manifests and charts.

## Grading

- Every experiment config uses the SREGym judge `judge_model_id = "codex-gpt-6-luna"`. The Codex CLI judge backend (`third_party/sregym/llm_backend/codex_cli_backend.py`) runs it at `xhigh` reasoning by default (`JUDGE_REASONING_EFFORT`). Use the same judge for every arm of a comparison.

## Timing metrics

Report timing without judge time, the same way for every arm (`benchmarks.sregym.analysis.incident_cost` computes all of these):

- **Time to diagnosis (TTD):** `diagnosis_submitted_at - fault_injected_at`.
- **Time to mitigation (TTM, the headline):** `mitigation_submitted_at - fault_injected_at`, minus the diagnosis-grading wait `fault_injected_at + TTL - diagnosis_submitted_at`, and never less than when the agent's last state-changing command before its mitigation POST completed, taken from the exported Codex rollout. The floor matters because an agent that keeps repairing while the judge grades its diagnosis would otherwise be undercounted. TTM is unknown when the CSV lacks `diagnosis_submitted_at` or `TTL`.
- **Supplementary only:** the raw `mitigation_submitted_at - fault_injected_at` (`raw_incl_judge_s`, which includes diagnosis grading), and the CSV's `TTM` column (which also includes the mitigation oracle). Do not present either as a headline number.
- None of these times includes SDO reflection, lifecycle or controller installation.

## Run assurance

- Every launch runs the preflight in `runner/preflight.py`. It aborts on low disk, floating or mismatched Codex CLI and agentshim pins, broken images, lane-isolation problems, any role or judge off `codex:gpt-6-luna` / `codex-gpt-6-luna` at `xhigh`, arm-parity differences, or an exhausted Codex quota. Check the arms of a comparison before launching them: `uv run python -m benchmarks.sregym.runner.preflight <arm.toml>...`.
- Reused images must prove their runtime entrypoints before injection. Controller/validator images in fastloop remain bound to the run source revision; an unlabelled or older responder must pass the real Python entrypoint import smoke, including the required benchmark recorder/transport imports. SREGym experiment and fastloop preflight checks select that explicit benchmark profile; production responder image checks keep the production profile. Runner image checks also accept the `benchmark_image` role. For development mounts, the smoke uses the current source projection returned by production `devmount.ApplyWorker`, including its benchmark file allowlist; checking only the older image entrypoint cannot establish compatibility. A missing transitive module rejects the named image before worker discovery.
- Shared Linux kind hosts need `net.ipv4.neigh.default.gc_thresh3 >= 65536`; the launch preflight rejects smaller values. Neighbor exhaustion can stall node loopback API/etcd connections as well as pod traffic. An administrator must approve any host-wide runtime `sysctl -w` increase; preflight never changes kernel settings. For remote Docker or hosts without the Linux proc knob, verify capacity directly on the Docker host (the check reports `unknown`).
- Every run directory gets a `run_manifest.json` (`runner/manifest.py`).
- Report only runs that `uv run python -m benchmarks.sregym.analysis.run_validity` classifies as `valid` or `agent_failure`. `incident_cost` excludes `invalid_infra` runs itself and prints why. Do not rename run directories by hand to mark them invalid.

## Token metrics

Report tokens the same way for every arm (SDO responder, SDO reflection, one-time lifecycle, and the raw Codex or Claude Code baseline); `benchmarks.sregym.analysis.incident_cost` computes all of these:

- **Breakdown (agentshim >= 0.7, identical for Codex and Claude):** uncached input, cache reads, cache writes (and the one-hour-TTL part), output, and reasoning. `input_tokens` includes cache reads and writes; `output_tokens` includes reasoning.
- **Raw total:** `input_tokens + output_tokens`. Always keep it beside the weighted total.
- **Cost-weighted total:** each token class times its weight, in base-input-token units, plus USD where the model is priced.
  - The weights come from agentshim's static pricing table (`agentshim.default_pricing()`, sourced and dated per entry), keyed by the provider and model in the experiment's `experiment_config.toml`.
  - gpt-6-luna: cache read 0.1x, cache write 1.25x, output 5x. Claude Haiku 4.5 / Sonnet 5 / Opus 5: read 0.1x, 5m write 1.25x, 1h write 2x, output 5x. Opus 5.5 reads at 0.05x and Fable 5.1 at 0.025x.
  - State the table version and date (the report prints `prices: ... last updated ...`) and treat the weights as an assumption.
  - Override with `--weight CLASS=MULTIPLE` or `--pricing-table`; USD always uses the table.
- **Requests per incident:** model requests per responder and reflection session (Codex `token_count` events with usage in the rollout; Claude `num_turns`), and per baseline run.
- **Sources:**
  - Codex numbers come from the exported rollouts (per-request `last_token_usage`), which carry reasoning and request counts that receipts written before agentshim 0.7 lack. Receipts are the fallback.
  - A Claude baseline is read from its `stream-json` result frames, because SREGym's own `usage_metrics` for Claude excludes cache reads from input and drops cache writes.
  - Old SDO records whose `cached_input_tokens` counted Claude cache writes are split back into reads and writes by `agentshim.TokenUsage.from_dict`.

The fault-observation tooling map and offline plan gate are documented in
[`faults/README.md`](faults/README.md).
