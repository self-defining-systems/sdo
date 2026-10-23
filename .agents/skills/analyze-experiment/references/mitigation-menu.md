# Mitigation menu receipts

## Implementation status

The version 1 messages in `mitigation_menu.proto` and the `sdo menu`,
`sdo mitigate --selection FILE` and `sdo report` CLI surface are a foundation.
The runtime implements report retrieval and private session/checkpoint
handshakes, with a generated `SelectionLedger` in the separate incident
`*-tools-state` ConfigMap. `menu` and `mitigate` execution remain unavailable.
The worker resume protocol is gated by `SDO_MITIGATION_SESSION=1`, which the
dispatcher does not yet enable. Red B3 and credential acceptance fixtures
identify the missing executor; passing ledger/session tests do not establish
menu execution, causal freshness, credential safety or closure readiness.

## Identity and settlement

- A selection binds incident, provider session, dispatch epoch, menu/option IDs,
  program/dependency revisions, accepted source, scope/policy digests, named
  bindings and a round limit of one or two.
- `request_id` is the semantic retry key, independent of the request filename.
  The canonical payload hash normalizes the transport epoch to a constant and
  sorts binding names; changing the transport epoch does not change semantics. Reusing a key for changed semantics must refuse.
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

## Reading the incident journal

`ControllerState.selection_ledger` is written through the same CAS storage edge
as the legacy counter journal, in a separate incident document. The controller
lifecycle document must not overwrite it. Inspect `used_batches` and
`reserved_batches` together: a reservation is not yet a settled application.
Each attempt retains its immutable acceptance epoch, semantic hash, actions,
rounds, barriers, terminal report and delivery/closure acknowledgments. A source
history entry does not prove a new investigation or a fresh verification.

A recovered action without completion is `STATE_UNKNOWN`; never replay it or
infer absence from a lost reply. Unknown calls charge the entire reservation.
Current recovery without a direct-reader callback settles `STATUS_INTERRUPTED`
with `recovery_observation_unavailable`. This is visible failure, not a passing
barrier. `writes_frozen` survives adoption with unproven quiescence; legacy
apply/rollback gates cannot authorize writes in that state.

The provider checkpoint stores only identity, transcript-relative path, prefix
length and digest in the existing session scratch volume. It requires the same
persisted transcript prefix on resume; a later append is allowed, replacement
or truncation is not. Missing/changed checkpoints must interrupt visibly.
The report outbox drains only after the original provider session acknowledges
delivery; publication into a transport response file alone does not drain it.
Raw `act` fencing, health-refresh continuity and fresh closure integration
remain outstanding; do not interpret these records as proof they are wired.
