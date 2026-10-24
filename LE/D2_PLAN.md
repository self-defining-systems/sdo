# LE-D2 implementation plan

Started 2026-10-24 (UTC). Work only in le-d2; base 51d508af7.

1. Add fail-first transport and CLI tests for fresh, read-only `sdo detect` (all/repeated selected IDs; unknown IDs list valid IDs), then implement it.
2. Add controller/tool closure-parity fixtures (healthy, health firing, retained drift, prober unavailable). Share the controller closure verdict and route status_cli through the same tool; preserve fresh health/traffic observation and accepted-source drift checks.
3. Add typed continuation `still_broken` and `already_done` lists, including program applications and recorded-action receipts, persist both through existing continuation state, and render the resumed user turn from a Markdown template (instructions <=1500 characters).
4. Add hermetic Gate 1 B: three independent faults, two repaired, same-session continuation contains only the remaining finding and both repair receipts, then final repair closes. Exercise detect and verify through the real session transport.
5. Merge latest dev, run formatting/static checks, focused race/Python tests and the hermetic gate, commit/push passing steps, open PR, and publish D2_READY and D2_HANDOFF.md.

Risk: command protocol, closure classification, continuation/restart evidence. Required validation: Tier 0 contract fixtures plus Tier 1 scripted controller/transport checks (no model, cluster, Docker or secrets). Tier 1 on the integrated commit gates any later live run. Prompt plumbing is scripted here; this does not claim live model behavior.
