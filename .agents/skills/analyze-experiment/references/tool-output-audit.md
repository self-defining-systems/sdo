# Compact responder output and offline audits

SDO incident commands print compact human summaries by default. Complete JSON
is written privately to `/tmp/sdo/<incident-id>/<sequence>-<command>.json`,
named by the `Full JSON:` line; `--json` emits that complete response for
scripts. A summary can omit list items or clip text and records omission
counts. Its `healthy`, `targeted_healthy` and command success remain distinct;
only the controller closure check establishes global recovery. Accepted
playbook README references do not imply executable mitigation capability.
Temporary files belong to the worker container, so an exported rollout may
retain a path after its full file has disappeared. Use durable tool receipts
and outcome evidence for attribution. Incident facts files live through the
response turn; accepted README files are private turn-scoped references.

Before investigating token growth, run the offline audit:

```bash
uv run python -m benchmarks.sregym.analysis.tool_output_audit <rollout.jsonl> --threshold 3000
```

It lists every oversized function/custom tool output by ordinal, tool name,
SDO command names, Unicode character count and UTF-8 bytes, without printing
the content or command arguments. One tool output may bundle several shell
commands; do not attribute its whole size to the first SDO command. Compare
per-request cost-weighted input as `input_tokens - cached_input_tokens +
0.1 * cached_input_tokens` for gpt-6-luna; keep raw tokens and output tokens
separate. Defaults and incident summaries have measured `tool_stdout.*`
budgets in `sdo/agents/prompt_budgets.toml`; `--json` is intentionally excluded.

