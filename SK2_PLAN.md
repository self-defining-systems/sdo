# SK2 sub-step plan

Base: origin/vic-lsh/dev/sdo-improve at 4e4ef26e5.
Scope: RV7 steps 1–2; no metrics, lanes, live runs or credential reads.

1. Add fail-first authority contracts, then a bounded versioned observation scope and installer-owned ceiling. Wire trusted policy into broker/CLI/runtime and narrow controller-only read grants. Push.
2. Add fail-first credential canaries and safe allowlist projections for foreign Deployment, Service, Endpoints and named admission webhooks. Add SDK/provider/capture replay contracts; update schema pins. Push.
3. Wire accepted-commit-bound scoped observation into cache/scheduler and qualified drift/source admission. Track per-target availability; denial is Unknown, retain baselines through gaps, relist before restoration. Add seeded revoke/regrant and namespace-collision checks. Push.
4. Run focused tests, capped redaction fuzz, seeded availability simulations, format/check scripts and independent adversarial review. Update architecture and analysis references; write the external SK2_REPORT.md; push and open a PR to dev without merging.

Validation risks: authority, credentials, failure/restart handling and platform wiring. Required offline levels: L0–L5 and L8. Real-cluster validation is excluded by the brief; record that limit explicitly.
