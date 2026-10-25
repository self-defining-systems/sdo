# Mitigation menu receipts

The menu is a convenience over direct commands. The controller enables it with
`SDO_RESPONDER_MENU=1`; unset or `0` leaves it off. `menu` lists accepted Mitigators
with declared object/value parameters and registered raw actions as one-action plans.
`mitigate OPTION [--NAME VALUE ...] [--rounds N]` uses the ordinary PlanExecutor,
existing controller RBAC and always-on executor checks. There are no menu-specific
selection-ledger, fencing, quiescence, provenance, health-reader or credential-delta gates.

## Reports and incident evidence

A typed report identifies the incident/session, request/attempt and each round/action.
Read what ran, each action's result, value provenance and the error together.
`APPLIED` means an effect landed; `NOT_APPLIED` and `UNKNOWN` never prove recovery.
Ordinary executor compensation is reported separately. No raw pre-image or credential
value is required to interpret these facts.

Reports return into the same responder session. A failed option may be followed by
another option or direct repair. Up to five rounds share the incident allowance across
options and ordinary plan batches; smaller configured limits remain effective.
Session identity, reports, repair receipts and counters use the existing tool-state snapshot. Immutable plan/application repair receipts persist at completion, before continuation feedback exists.
Session/checkpoint messages remain presentation state, not mutation authority. There is no
menu ledger, report outbox or automatic replay; historical ledger fields are retired on this path.
Fully applied options that fail verification remain applied. Each execution identifies its
application and states whether changes remain, with the error and compensation status.
The existing explicit rollback command works for in-process menu applications; rollback
preimages are never persisted or restored. Only partial write failures auto-compensate.
A report query reflects a later explicit rollback; already returned response snapshots stay stable.

Menu action receipts enter `IncidentResult.repair_actions` with action identity,
resources, timing, details and success. Menu reports are not legacy PlanApplication
proof and add no positive program trust credit. Intermediate rounds are not independent
incidents. Terminal menu success alone is not controller health verification, broker
ACK or a strict benchmark receipt.

## Mitigation health scope

`apply` and `mitigate` return after their writes and touched-resource rollout checks, then take one health observation; they never poll global health while unrelated faults remain. The built-in verifier retains the original target finding fingerprints independently of the written objects. Explicit repair scope (`--finding FINGERPRINT` on plan/mitigate) is validated before writes; otherwise authored finding/playbook links and coverage supply targets. Unidentified scope requires the whole single sample to clear. Unavailable health observations fail closed.

Live responses include `mitigation_health` (written resources, original target fingerprints, current health findings, targeted findings and both health verdicts) and `targeted_healthy`. A cleared program diagnosis also gates the menu's targeted success. `STATUS_SUCCESS`, plan `verification_passed` and `targeted_healthy` describe the completed mitigation, and may coexist with `healthy: false` and named peer findings. Round `independently_healthy` retains the whole-health observation. Truncated tool records retain `targeted_healthy`; scope/findings may be omitted by the existing record size cap. None of these facts independently proves incident closure, broker ACK or a benchmark receipt. Read `sdo verify`/controller closure evidence for global recovery.

## Scripted production check

The hermetic production test goes through real `RunWithOptions`, `LoopFactory`,
KubernetesJobDispatcher, Python CLI and the native broker with a synthetic Kubernetes
API. It selects a failing option, consumes its typed report, selects a successful
alternate in the same session, then requires independently derived success, broker ACK
and the benchmark adapter's strict receipt. This proves interface wiring, not live
cluster behavior or admission policy.

A missing required repair receipt field previously rejected the enriched dispatch
result; retry then lost process-local evidence and closed as `cleared_without_action`.
Read classification and ACK separately. The menu now supplies required receipt details,
and a focused incident-contract test guards that failure before the end-to-end check.

For a logical program ID with several finding-derived menu rows, explicit CLI `--param NAME=VALUE` / `--NAME VALUE` inputs now request a controller-bound menu for that program before selecting one concrete option. Go parses object and scalar bindings and rejects unknown or incomplete required parameters. Without explicit parameters, multiple rows remain ambiguous; a concrete option ID still selects that exact row. A binding/listing failure consumes no mitigation batch.

The reviewed mixed10 source policy includes the deployed chart identity `opentelemetry-demo` mapped exactly to `astronomy-shop`, alongside `astronomy_shop`. A scope refusal with adopted memory can be an application-identity mismatch rather than missing corpus evidence: compare the incident's `application` and `namespace` with the committed policy map. The control namespace is not an application repair scope. Accepted-source contents and metadata UID checks still govern each action.


## Unexplained object removal and failed receipts

`remove-unexplained` menu options bind the exact finding, resource UID and resourceVersion. Go rechecks current accepted-source absence, onboarding acknowledgement, SDO ownership and control namespace at execution; the controller performs the conditional delete. Worker permissions are unchanged. Both namespaced and relevant cluster inventory objects use the same path. Reports retain exact object identities and ordinary tool/application audit evidence.

`RepairActionReceipt.failure_reason` and `ActionReport.failure_reason` distinguish `authorization_denied`, `execution_failed`, `delete_not_confirmed` and `outcome_unknown`. A command exit zero alone does not prove every declared deletion: CLI readback and independent Go ingestion check absence. Unconfirmed effects earn no success receipt. Continuation `already_done[].receipt.failure_reason` retains failures across restart. After one continuation with the exact unchanged broken set and typed authorization denials on every remaining resource, the result is failed with `error=authorization_denied_no_progress`, without claiming loop-budget exhaustion or recovery. Failed-decision timestamps remain distinct from verified recovery.

`IncidentResult.unresolved_findings` records Go's terminal verification and is omitted from worker model output. Terminal unchanged source-absent objects without pending deletion or GC can close failed immediately, preserving grace for changing/recovering state. Inventory fingerprints include observed incarnation/configuration values; status/resourceVersion-only churn is excluded. Tool output distinguishes `mitigation_rounds_used/allowed` from `response_loops_used/allowed`; legacy `loops_used/allowed` describe write batches. Final `response_loops_used` counts provider executions. A missing API transport is refused before dispatch and never claims `changes_remain`.
