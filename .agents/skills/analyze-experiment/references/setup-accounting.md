# Setup accounting and restoration grades

Whole-object restoration grades strip API-assigned fields using the same
`adapter.kubernetes_content` projection as application reset. A recreated Service
may receive a different ClusterIP or nodePort without losing restoration credit;
headless identity (`clusterIP: None`), labels, selectors and target ports still count.
Historical grades written before this projection are not silently corrected.

Prelude failures now write non-injected canonical rows for all unstarted slots,
including the original exception. Raw no-fault rehearsal verdicts are retained in
`oracle-rehearsals/NNN.json`; do not infer HTTP failure from the generic error alone.
`pre_injection_responder_tokens` counts onboarding responder worker results created
in this resolve call before injection, separately from incident responder usage.
Add it to responder totals once. Null with `pre_injection_usage_error` means the
worker result accounting was unavailable; it is not zero. Existing setup/lifecycle
and reflection accounting remains separate.
