# D1 implementation plan
Started 2026-10-24 23:15 UTC. Risk: incident ownership, durable contracts, restart behavior, reporting. Required: tier 0 contracts/race tests and tier 1 hermetic gate. Tier 1 gates subsequent live work. No live models or clusters.

1. (≤30 min) Add fail-first coverage/no-automatic-apply contracts; remove controller fast path and flags, preserving explicit rollback and single-apply compensation. Verify focused Go compile/tests.
2. (≤30 min) Add typed deterministic coverage from initial diagnosis, resource joins and informational outcome counts; persist coverage and regenerate contracts. Verify coverage/round-trip tests.
3. (≤30 min) Add three-fault scripted incident through real tool menu/recorded actions; sweep consumers, reporting and docs. Verify scenario/focused Python tests.
4. (≤30 min) Merge current dev; run formatting/static, focused race and hermetic checks; fix failures, push PR and handoff.

Decisions and evidence go in the handoff. Push each passing implementation commit immediately.
