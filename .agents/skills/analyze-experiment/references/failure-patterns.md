# Common SDO experiment failure patterns

## Incomplete evidence chain

**Signature:** benchmark output reports success but the strict receipt is missing, invalid, or cannot be correlated to an outcome and broker ledger.

Check incident IDs, commit hashes, final detector state, independent verification, acknowledgement, cleanup, and remaining worktrees. Report benchmark success and production completion separately.

A receipt whose `resolution` is `external_recovery` or `cleared_without_sdo_action` is valid but is not an SDO mitigation: count it under "cleared without SDO action", never in the mitigated total, whatever the oracle says. In fastloop records the same value is `sdo_resolution`. Older runs rejected such receipts (`sdo_rejected_production_receipt.json` with `same_session_reflection=true` or `completed=true` required); reclassify them from the rejected receipt's own fields instead of counting a product failure. A remaining rejected receipt is that stage's `agent_failure` only: it no longer blocks the next stage or fails the teardown.

## Outcome-layout or broker-mode mismatch

**Signature:** an investigator counts both imported `.sdo/outcomes/*.json` documents and the retired `.sdo/outcomes.jsonl` stream as independent incidents.

Use the logical incident ID and `sdo/operator/memory` semantics; the JSON filename is `sha256(incident_id)`, not the ID itself. Import is restartable and removes the legacy stream only after every record is accepted. Trusted source evidence is broker-owned, tied to the accepted Git tree and immutable images, and independently re-observed after restart. A worker-reported `deployed` flag or image annotation alone is not evidence. For historical JSONL runs, inspect the accepted Git revision before comparing outcomes.

## Lifecycle provenance mismatch

**Signature:** deployer commit or topology fingerprint differs from the current application workspace, judge sessions are reused unexpectedly, or the objective digest differs from `goal.md`.

This indicates stale or fabricated bootstrap memory. Check whether a pipeline lifecycle seed was copied and whether reuse validation ran against the correct source revision.

## Repeated deployment rejection

**Signature:** bounded source-deployment attempts receive the same verifier feedback or fail to create an attributable commit.

Look for unchanged Git HEAD, missing role trailer, Kubernetes rollout errors, agent claims unsupported by cluster state, and verifier feedback that the next attempt ignored.

## Detector quality or build failure

**Signature:** generated Go fails manifest validation, compilation, tests, or near-miss behavior.

Check ownership/class, objective digest, watches, namespace handling, source-backed resource coverage, nondeterministic dependencies, and whether tests merely restate implementation details.

## Benchmark-tailored seed memory

**Signature:** the run's lifecycle seed predates the de-tailoring of benchmark-specific prompt hints. Its `.sdo/goal.md` says "required non-optional ConfigMap volume references remain present", its health detector has a `network-policy-total-isolation` rule, or its health-objective manifest watches lack `apps/v1 ReplicaSet`.

Such a seed (for example `30e023d`) carries checks written knowing the SREGym faults. Its detection and diagnosis results are not a fair SDO measurement; report them as pre-fairness, and use a seed from a fresh lifecycle on the de-tailored code.

## Finding-state instability

**Signature:** incidents fire or clear unexpectedly, duplicate dispatches occur, or restarts change behavior.

Inspect firing/clearing thresholds, debounce state, snapshot versions, durable controller state, leader transitions, and incident idempotency keys.

## Link reachability finding (latent dependency fault)

**Signature:** a finding with rule `link-reachability.<from>.<to>.<port>` (detector `traffic-<workload>` for a `purpose: link-probe` workload in `.sdo/diagnostics/traffic/workloads/`) while every `scenario-slo.*` scenario stays green.

The prober dials each declared edge afresh every interval and the finding needs `failures` consecutive failed dials of an edge that had connected before. The caller may still hold an old connection, so user-facing probes can stay healthy for the whole fault. Check that the edge and port are grounded in the application source, that the fix restored fresh connectivity for the same edge (the finding must clear), and that no finding appeared for an edge the prober never reached (those are never reported: the prober's own identity may simply not be admitted there). Detection time is the first failed dial plus about `failures` intervals; a link detector should not carry the scenario detector's 9 s minimum duration.

## Responder detour or thrashing

**Signature:** the responder performs broad exploration, repeats hypotheses, changes unrelated source, or alternates between fixes without improving detector/health evidence.

Compare trajectory actions with findings, surfaced playbooks, architecture, and validator feedback. Count proposals and rejected commits rather than only model turns.

## Red-herring diagnosis

**Signature:** health verifies, but the outcome's or receipt's `diagnosis_verification` shows `contradicted`, `unverified`, or `unattributed`, or the outcome is `external_recovery`; or the diagnosis names a decoy such as a `failure-admin-*` ConfigMap that never appears in `request.state_changes` or `final_state_changes`.

Compare the cited evidence with the closure's `state_changes` (both `request.state_changes`, the dispatch-time snapshot, and `final_state_changes`, the verification-time diff) and `incident_detector_states`. A cause built on `static_context` alone, or one whose `explained_detectors` never cleared, is a lucky fix, not a learned one. Reflection should not have saved a playbook from it; check the reflection commit. On a composite fault, a `contradicted` state-change citation that appears in neither diff is a real red herring; one that appears only in `final_state_changes` (a later component landing after dispatch, as in K2) is expected to verify, not contradict — if it still shows `contradicted` in a ledger this old, the run predates the verification-time state diff. An `unattributed` cause had its detectors flip, but no successful responder repair that started before `health_cleared_at` touched its resources, or a dispatch-diff change was reverted by something else (`repair.externally_reverted`): someone else fixed the fault, and the responder's claimed cause is not evidence. An outcome whose causes are all unattributed is `external_recovery` and is never learned. A `contradicted` `state-change` check whose `reason` says it was first observed after the responder's own repair (rc2) is the responder citing its own edit; the fault itself was probably outside the configuration diff.

## Ownership or broker rejection

**Signature:** a repair appears plausible but the broker rejects it.

Check edits to human goals, architecture, health detectors, prior outcomes, or paths outside the allowed worktree; missing provenance; dirty or uncommitted source changes; detector validation; and stale branch heads. Under `recorded-actions`, distinguish a legitimate live-only repair with a successful action receipt from an unrecorded mutation; source changes still require a proposal commit.

## Reflection without learning

**Signature:** an outcome commit exists but reflection is absent when required, uses a different session, produces no validated memory commit, or generalizes beyond evidence.

Correlate classification, health verification, responder session ID, outcome commit, reflection commit, accepted detector paths, and controller rollout. Reflection starts fresh by default; explicit resume may fall back when its provider session is unavailable. Direct-edit reflection workers author final checkout files, run `memory check`, and return decisions and logical IDs per dispatch. Go records `reflection_rejections` and re-dispatches with typed memory diagnostics at most twice; `reflection_rejected` marks exhaustion. A gap-only decision has `learning_decision=sdk_gap` and typed `sdk_gaps`, with no reflection commit. Fastloop retains mitigation evidence even when learning fails. The CLI and broker use the same `memory/validate.Check`, including program/corpus policy and replay; compare recorded file edits, named diagnostics and the exact accepted diff. Proposal-only archived recordings cannot replay on the direct-edit path.

A verified ordinary drift repair still learns through direct edits: its closure carries the checked responder drift verdict and its corpus uses the original `open@detect` capture, even if a later pre-apply capture is already healthy. Verify that the local `memory check` and broker both use the private incident capture directory.

For a newly learned program, a matching corpus case without `labels.json` capture provenance is a circular test: the agent may have reconstructed both evidence and expectation. Compare the labeled open and fixed digests with the private session captures and verify the healthy peer binding points to a same-kind object in the fixed snapshot. `synthetic_no_match_reason` permits a recorded tightening near miss only; it does not establish a real incident match. Older runs with unlabeled reconstructed snapshots should be identified as pre-capture evidence.

## Learned detector never fired

**Signature:** a later variant of a fault the responder learned a detector for is solved cold: the receipt has `detector_firing_available=true` but `incident_detector_fired_before_dispatch=false`, so the stored memory went unused for detection.

Distinguish by evidence: `no_incident_detector_fired=true` means only health detectors fired. A `not_persisted` record in `detector_firings.jsonl` for the learned detector means it flagged the fault but did not reach its firing threshold. `incident_detector_fired_after_dispatch=true` with `before_dispatch=false` means it fired only after the responder was already dispatched (too late to shape the request). No record at all for the detector means its predicate did not match the variant (over-specific parameters or selector; compare the record's `parameter_bindings` with the prior incident's) or the controller was not running that detector version (check the rollout and `detector_id` in the manifest). Receipts without `detector_firing_available` predate the telemetry and cannot answer this.

## Benchmark leakage

**Signature:** lifecycle, detector, controller, or responder evidence contains fault labels, verdicts, submission APIs, or hidden benchmark metadata.

This invalidates the production path. Benchmark transport belongs only in the SREGym adapter.

## Healthy evidence chain

| Layer | Expected evidence |
|---|---|
| Lifecycle | Distinct fresh sessions, current source commit/topology, validated objective detector |
| Controller | Persistent finding history, one correlated incident, durable state |
| Responder | Isolated worktree, structured result, attributable proposal |
| Broker | Ownership checks, independent verification, committed outcome |
| Reflection | Same-session proposal when applicable, validated memory commit, correlated rollout |
| Benchmark | Explicit diagnosis and mitigation success plus one valid strict receipt for SDO adapter runs |

## Native judge direct edits

For `health-judgment-direct-v1`, inspect the proposal checkout diff and accepted
judge commit for health detector, traffic and health-objective playbook content.
The typed result contains metadata and logical detector IDs; an artifact payload
is a rejected retired contract, not evidence of authored files. Both the CLI and
broker use `validate.Check`, including the explicit Go `memory.PrepareHealth`
step for generic service-endpoint and traffic policy detectors. Accepted commits
include those prepared files. A green authoring check does not establish health:
correlate the separate independent verifier and installation evidence. Judge
refinements can change health diagnostics after bootstrap but cannot change
source, human goal, controller outcomes, incident detectors or other playbooks.

Go dispatch is the reflection replay unit after the mainline cutover. Each checkpoint replays one worker result through the shared validator and real broker; broker rejection/retry history remains in the Go ledger. `PlanApplication` retains controller-owned rollback pre-images, concrete applied actions, patches and timing through ProtoJSON restarts; `ProgramToolCall` retains its summary.

For latent drift or toy lane failures, read [drift and toy failure patterns](drift-failures.md).
