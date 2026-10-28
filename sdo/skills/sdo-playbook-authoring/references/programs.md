# Direct program authoring

Programs implement the [generated program SDK](program-sdk.md). Diagnose reads a bound resource view;
mitigation returns typed actions. Import shared SDK helpers (`sdk/observe`, `sdk/routing`) instead of copying them into `.sdo/lib`.
Keep names, ports and images in bindings or observed state, never in the learned predicate.

Detectors detect continuously; playbook Diagnose runs only while handling their findings, using one finding-bound assignment. Read additional objects through ReadView by name or relation.
Detectors call `ctx.DiffAgainstSource(ref)` before selection, then bind playbooks through findings.
`Changed` covers field drift; `SourceAbsent` confirms absence. Unavailable evidence errors.
Both contexts share recorded comparisons.

Accepted Mitigators and raw one-action plans appear in the responder menu.
Declare object/value parameters; group related actions. PlanExecutor retains its checks; SDO identities hold a cluster-wide grant, so RBAC refuses nothing.
Per-action results, provenance and errors guide the responder across five incident rounds.
Intermediate reports grant neither independent outcomes nor trust credit.
Mitigate may run again: when its rollout stays pending, the runtime re-plans on fresh observations and
applies the next plan until it repeats an earlier plan (at most three follow-ups). Write a repair
that depends on what its write causes as one Mitigate over current state, e.g. also retiring an old-template
ReplicaSet once new pods are Unschedulable beside it. Select that ReplicaSet by its controller ownerReference to
a bound Deployment and an older `deployment.kubernetes.io/revision`, never by name; any other ReplicaSet scale is
refused as `replicaset_scale_refused`. Once a stalled rollout is released, a later responder write ends the
program's credit for it. Prose in the README never runs.

A program directory contains diagnose.go, optional mitigate.go, Go tests and a README with responder ownership,
fault class and originating incident. Preserve accepted provenance and add its logical ID to the playbook index.
The authoring reference tool supplies the routing example.

## Topics

- [Evidence and replay fixtures](programs/evidence-and-replay-fixtures.md)
- [Recorded repair coverage](programs/recorded-repair-coverage.md)
- [Source restoration](programs/source-restoration.md)
- [Values and runtime actions](programs/values-and-runtime-actions.md)
- [Routing](programs/routing.md)
- [Acceptance](programs/acceptance.md)
- [Go-owned per-cause work](programs/go-owned-per-cause-work.md)
- [Removing one Service port](programs/removing-one-service-port.md)
- [Database catalogs](programs/database-catalogs.md)
