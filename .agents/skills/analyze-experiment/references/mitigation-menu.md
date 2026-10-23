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
Counters use the existing tools-state persistence. Session/checkpoint messages are
presentation state, not mutation authority. There is no menu ledger, outbox or durable
report-redelivery guarantee; historical ledger fields are retired on this path.

Menu action receipts enter `IncidentResult.repair_actions` with action identity,
resources, timing, details and success. Menu reports are not legacy PlanApplication
proof and add no positive program trust credit. Intermediate rounds are not independent
incidents. Terminal menu success alone is not controller health verification, broker
ACK or a strict benchmark receipt.

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
