# Mitigation menu receipts

## Implementation status

The Go responder loop serves `menu`, `mitigate` and immutable reports through
the existing tools transport. `sdo mitigate OPTION [--rounds N]` selects from a
fresh menu; complete JSON selections remain available for semantic retries.
`MaxBatches` / `max-response-loops` is the single incident-wide allowance,
default 5, shared with legacy apply and recorded act. A round that reaches a fresh
observation spends allowance even when its plan is refused before an effect. Each round uses direct
current observations in the original session. Controller-supplied safety and
independent health capabilities are mandatory; missing capabilities refuse
before reading content. Synthetic B3/Secret acceptance passes do not establish
production policy provisioning, health-refresh continuity or live stream readiness.

## Identity and settlement

- A selection binds incident, provider session, dispatch epoch, menu/option IDs,
  program/dependency revisions, accepted source, scope/policy digests, named
  bindings and a round limit within the remaining configured incident allowance.
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
  are separate. `action_kind` and `changed_fields` name safe changes without
  storing field values. Unknown never proves no effect or successful restoration.
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
prove content safe. A generic SDK-plan effect delegates the vocabulary to the existing executor;
every effect requires forward/compensation/evidence safety authorization. This is the menu's guarantee,
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
Configured menu recovery captures direct read-only evidence even while the
replacement is frozen. Recovery without a direct-reader callback settles `STATUS_INTERRUPTED`
with `recovery_observation_unavailable`. This is visible failure, not a passing
barrier. `writes_frozen` survives adoption with unproven quiescence; legacy
apply/rollback gates cannot authorize writes in that state.

The provider checkpoint stores only identity, transcript-relative path, prefix
length and digest in the existing session scratch volume. It requires the same
persisted transcript prefix on resume; a later append is allowed, replacement
or truncation is not. Missing/changed checkpoints must interrupt visibly.
The report outbox drains only after the original provider session acknowledges
delivery; publication into a transport response file alone does not drain it.
Recorded `act` holds the mutation gateway until command completion. A missing
completion prevents subsequent writes. Health-refresh continuity and production
policy provisioning remain outstanding; do not infer them from ledger receipts.

## Direct causal component

`MitigationBarrier.collections` retains each fully read kind/namespace, opaque
collection resourceVersion and item count. Empty lists need these receipts too.
The direct Kubernetes reader requests most-recent namespaced lists; pagination,
missing readers and unobserved dependencies refuse completeness. ResourceVersion
identity is recorded, never ordered. UID replacement, a missing intended effect,
server defaults restoring a removed field, finalizers on a delete, and a source
fence changing during reads all fail the barrier. Creation observation requires
a durable prior-absence capture and the newly observed UID; no create effect is
enabled by this component.

Each capture has a durable incomplete intent before collection and a separate
completion CAS. A failed completion exposes no usable verification view. The
returned view contains the direct objects and rejects unlisted kinds; unavailable
traffic observations cannot masquerade as healthy. Read authorization must be
supplied by the caller before collection. These are causal checks, not proof of
nonsecret provenance. The menu uses the component before rounds, after writes, after compensation
and for closure verification; it does not replace global controller closure guards.

A resumed worker's turn timeout is the remaining original deadline; expiration
does not allocate a fresh deadline. Journal integrity checks reject missing
reservation totals, duplicate IDs, future acceptance epochs, incomplete provider
identities and contradictory action facts. A persisted write prohibition allows
read-only report redelivery after another restart; it differs from the local
storage-failure fence, which cannot safely continue that owner.

The menu calls the plan path's single credential/identity post-image delta predicate
regardless of action path spelling; independent content, consumer and
admission authorization remains mandatory around it. A successful scripted
hermetic controller/Job/broker run proves interface wiring and can produce a
strict benchmark receipt; it does not prove real-cluster admission or policy provisioning.

## Production launch wiring

`SDO_RESPONDER_MENU=1` on the controller process enables `run.go`'s real
`LoopFactory` menu configuration; unset/`0` leaves it off. The standalone launch
has no producer of collection-wide nonsecret/consumer/admission/routing evidence.
Its catalog-only disabled options report `collection_consumer_admission_provenance_unavailable`
(or `plan_safety_authority_unavailable` when reviewed policy is absent/stale).
They make no diagnosis or execution claim. Trusted embedded `RuntimeOptions.MenuSafety`
callbacks can supply the existing capabilities, still bounded by the separately
reviewed source-bound policy. Synthetic embedded e2e evidence is not live-stream
provenance. Full-scope direct lists, independent health, UID and credential checks
remain mandatory; omitted producers are never interpreted as healthy empty data.
The production adapter refuses unpinned creation identity before any write;
this appears as `plan_preflight_refused` in a selected attempt. Full effect
preflight also refuses a multi-action plan if a later action lacks evidence,
without first applying its earlier actions. SDK creation via direct apply
retains its existing separate authority checks.
