# Mitigation menu receipts

## Implementation status

The version 1 messages in `mitigation_menu.proto` and the `sdo menu`,
`sdo mitigate --selection FILE` and `sdo report` CLI surface are a foundation.
The runtime does not yet implement these commands. Red B3 and credential
acceptance fixtures identify the missing executor; a green schema or CLI test
does not establish menu execution, recovery or safety.

## Identity and settlement

- A selection binds incident, provider session, dispatch epoch, menu/option IDs,
  program/dependency revisions, accepted source, scope/policy digests, named
  bindings and a round limit of one or two.
- `request_id` is the semantic retry key, independent of the request filename.
  The intended canonical payload hash excludes transport epoch; binding order
  must normalize by name. Reusing a key for changed semantics must refuse.
- A report binds `attempt_id`, `request_id`, incident/session and immutable
  `acceptance_epoch`. Adoption must not rewrite acceptance identity or reset
  the original deadline, incident budget or provider session history.
- `STATUS_SUCCESS`, `STATUS_REFUSED`, `STATUS_EXECUTION_ERROR`,
  `STATUS_UNRESOLVED`, `STATUS_DIFFERENT_CAUSE`, `STATUS_SUPERSEDED`,
  `STATUS_INTERRUPTED` and `STATUS_TIMEOUT` are terminal report statuses.
  Pending selections need a ledger lookup, not a fabricated terminal success.
- Action receipts distinguish `STATE_NOT_APPLIED`, `STATE_APPLIED` and
  `STATE_UNKNOWN`; compensation state, conflicts and residual resource refs
  are separate. Unknown never proves no effect or successful restoration.
- Terminal reports must be durable before publication; replacement workers
  redeliver the identical report into the resumed session. Missing or
  unresumable checkpoints mean visible interruption, not a fresh session.

## Evidence and trust

Barriers carry attempt/round/phase/occurrence capture IDs, direct observation
UID/resourceVersion tokens, effect/absence confirmation and observation time.
Treat resourceVersion as opaque. A capture ID or `complete` claim alone does
not prove causal freshness; correlate it with the direct-reader receipt and
the diagnosis/health dependency set. `open@detect` is not a substitute.

Reports contain no raw Kubernetes pre-images or log, command or probe bodies.
Verdict evidence still needs an independently checked nonsecret provenance
boundary before serialization. Resource-kind filtering and redaction cannot
prove content safe. Enable only scoped routing SetField and Deployment restart
after forward/compensation/evidence safety validation; legacy reconcile and
revision rollback remain disabled in the menu. This is the menu's guarantee,
not global confinement of inherited shell or `act` access.

Menu reports are not legacy `PlanApplication` evidence and earn no positive
program trust credit. Do not count intermediate rounds as independent failures
or terminal menu success as incident closure. Independent health, original
finding clearance and bound NoMatch must still pass a fresh closure barrier.
Unproven old-worker quiescence freezes further writes, including fallback;
unknown/interrupted/timeout is unresolved evidence, never permission to replay.
