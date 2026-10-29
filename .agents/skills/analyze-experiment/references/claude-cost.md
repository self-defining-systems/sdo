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

Role costs have `cost_state: measured | model-free | missing`. Only complete billing
is measured; partial/native counters remain in `transport_usage`, never substitute
for a complete dollar value. Missing costs render as missing, including default zero
counters. `responder_model_dispatched` and `reflection_model_dispatched` are separate
tri-state facts; the aggregate `model_dispatched` cannot identify the paid role.

Model-free requires incident-correlated positive native evidence: a verified
`deterministic` closure with no responder session, Job or nonzero usage, or an
explicit reflection skip with no reflection dispatch/session/usage. Standby
responders have no Job, so an empty Job list alone proves nothing. Session,
request or usage evidence overrules a zero claim. Complete peer billing and an
absent phase journal never prove that the other role was model-free.

`stream_accounting` reconciles multiple frozen host projections and writes
role-separated medians and paired repeat/first ratios by `mount_sha` and
single/multi fault count (`__nN__` in the generated problem ID):
`uv run python -m benchmarks.sregym.analysis.stream_accounting --input host-a.json host-b.json --output analysis.json`.
Each metric reports its own known `n` and missing count; zero first values have a
separate `zero_denominator` count and no ratio. Cross-build rows contribute only
to their own build's marginals, never to a within-build ratio. Only settled rows
contribute. First/repeat are pair labels, not chronological fault-kind encounter
ordinals. The output preserves input hashes and cutoff timestamps. Reconciliation
reads current receipt/session exports, which may advance after those snapshots;
freeze the input evidence too when an immutable census is required.

A reflection **worker** dispatch is not a model dispatch: the registered worker's
`native_memory_check_no_change` result returns before creating a provider process.
This proves model-free reflection only when every recorded attempt has that
reason, no attempt reports usage/session evidence, and no model journal contradicts
it. A generic `no_change` result is insufficient. Partial zero counters and
malformed usage cannot prove no model. Stream medians exclude non-injected held
placeholders even when their journal is settled.
Ratio `unpaired` counts missing/excluded encounter rows separately from pairs
whose existing row has missing billing. This includes cross-build pairs excluded
from a within-build comparison.
