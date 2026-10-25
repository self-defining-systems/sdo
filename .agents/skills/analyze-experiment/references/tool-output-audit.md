# Compact responder output and offline audits

SDO incident commands print compact human summaries by default. Complete JSON
is written privately to `/tmp/sdo/<incident-id>/<sequence>-<command>.json`,
named by the `Full JSON:` line; `--json` emits that complete response for
scripts. A summary can omit list items or clip text and records omission
counts. Action JSON preserves the existing receipt redaction policy. Its `healthy`, `targeted_healthy` and command success remain distinct;
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

Mutating command displays separate the action result from whole-incident
verification. They compare controller cached findings before/after by fingerprint
and evidence into `fixed`, `new` (possible regression), and `unchanged` sets.
Every invocation writes the complete output and evidence to
`scratch/sdo-reports/<incident>/<sequence>-<command>.json` inside its private
session. Native runtime-artifacts export admits only numbered act/apply/mitigate
report filenames from that exact subtree. The report contains `actions`,
`option_reports`, `findings`, `fixed`, `new`, `unchanged`; the printed schema is
generated from Go/proto types. Large inventories explicitly point to the full
arrays if their finding lines exceed the bounded display; verdicts and all set
counts remain visible. `incident_verified` adds an informational timestamp/message
to normal responses; it does not replace findings, states, reports or receipts.
Plain verify uses cached state; `verify --wait SECONDS` waits for stable closure,
a new/reactivated finding, or timeout. Detect remains the fresh evaluation path.

Mutation output accounting includes the emitted UTF-8 bytes, JSON envelope/escaping and final newline for act, apply, mitigate and rollback. Controller progress is authoritative; absent snapshots are unknown, while explicit empty arrays remain empty. Action-local findings never substitute for controller observations. Full errors remain in the complete report; long-path fallback copies retain the original session-scratch report for export.
