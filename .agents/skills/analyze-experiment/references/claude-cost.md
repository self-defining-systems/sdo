# Claude cost completeness

Use `uv run python -m benchmarks.sregym.analysis.claude_cost --input <rows.json> --output <analysis.json>`
for paired (`pairs[].first/repeat`) or flat analysis rows carrying `provider`, `model`, `incident_id`,
`results`, and, for composite rows with follow-up incidents, `injection_started_at`.
The reader includes each current-row session fragment once and excludes older exported incidents.
Codex accounting is unchanged. Never overwrite recorded run inputs; write analysis outside `results/` and `live/`.

Claude worker `claude-turns.jsonl` journals live under `SDO_SESSION_DIR` (or beside `SDO_TURN_USAGE_LOG`).
They record phase start/end/error, session identity, token classes and `usd_complete`, including failed turns.
Assistant `message.id` deduplicates streaming fragments and exported copies. Session `cost-state` counters
are differenced at phase boundaries; final recorded API responses beyond the CLI counter are priced with
agentshim's dated table. Legacy guard journals are read as evidence, without trial-name filtering.
A complete, error-free legacy CLI result can substitute for a missing transcript only when its explicit
cache TTL classes price to exactly its reported USD; a cumulative dollar counter fails that check.

Analysis rows expose `responder_usage.usd`, `reflection_usage.usd`, phase `usd_complete`, and row
`cost_complete`. Missing evidence yields `usd: null` and `cost_complete: false`. Cumulative billing can
prove USD while a historical cache-write TTL split remains unknown: `cache_write_ttl_complete: false`
and `cache_write_1h_input_tokens: null` preserve that distinction. `reconciliation_evidence` retains billing
bounds, collected USD and counter discrepancies; `transport_usage` preserves the original row counters.
The standard `incident_cost` pipeline report uses the same reconciler for Claude and exposes `cost_complete`.
Shared-session phase windows are exclusive: a reflection guard can start while waiting for the
responder writer to exit. Its start is not billing ownership. Reflection excludes requests through
the responder's terminal boundary; otherwise both phase costs can sum above the final provider
session counter. Analysis records `guard_overlap_seconds` and `phase_boundary_source` when clipped.
