---
name: sdo-playbook-authoring
description: Start here for reflection. Turn recorded hand repairs into validated playbook programs (diagnose.go, mitigate.go, a labelled corpus case), or return no_change.
---

# From hand repairs to validated programs

Work in the proposal checkout, in this order. Open other files only when a step or a diagnostic names them.

1. **Is anything new?** The prompt's per-cause work lists the open questions; the current outcome lists
   `repair_actions` (receipts) and `plan_applications`. If no hand repair is left uncovered and no applied
   playbook failed, call `check_memory` with `no_change` and a one-line `no_change_reason`, and finish.
   Update or retire (`status: retired` and `retired_reason` in front matter) an applied playbook that failed,
   left findings unresolved or needed a later hand repair.
2. **Map each hand repair to a step.** Every successful hand repair needs an executable step: the typed
   action matching what ran, or `RunScript` with the recorded bytes. README text and `sdk_gaps` never replace
   a step; work not encoded yet is not an SDK gap. One playbook per failure mechanism; partial coverage is fine.
3. **Copy the closest worked example** from `/opt/sdo/examples/playbook-programs/` (`playbooks/<id>/` and
   its `corpus/<id>/`): `deployment-source-drift`, `webhook-ca-sync`, `database-user-recovery`,
   `crashloop-script-repair` (RunScript), `service-targetport-mismatch`. Exact signatures are on the
   [SDK card](references/sdk-card.md), grouped by fault.
4. **Write `.sdo/playbooks/<id>/`**: `diagnose.go` (`Spec`, `Diagnose`), `mitigate.go` (`Mitigate`, same
   package and spec) and `README.md` with front matter `schema_version: 1`, `owner: responder`,
   `originating_incident: <incident id>`, `description: <one line>`. The README holds only judgement and
   ordering: when to use it, what first, what to check after. SDO adds `fault_class` and the index entry.
5. **Label one corpus case** per new program (rules below): `create_corpus` with the session snapshot and
   `repair_receipt_ids` (the receipts it encodes) writes it in one call.
6. **Check**: `check_memory` with `learning_decision: updated`, `proposed_artifact_ids` = the changed
   playbook IDs, and a summary. It formats changed Go first. Fix each blocking diagnostic as its hint says,
   then check again. Replay notes and `coverage_gaps` are information, not errors.
7. Return the same decision and IDs.

Keep revisions that applied cleanly unless this incident shows failure or incompleteness; add or narrowly edit, never rewrite working steps.

## Program rules

- Roles come from findings: one `ParamSpec` per object Kind. Read other objects through ReadView by
  relation (selector, owner, mount), never by app name; put unavoidable app facts in the README.
- Write values only as `FromSource`, `FromObserved` or `FromParam` (a declared scalar with a default).
- Diagnose returns Match on positive evidence, NoMatch when the fault is absent, and an error (Unknown)
  when a read is unavailable. Check what the action needs, e.g. that the cause is gone before a restart.
- Mitigate returns to accepted or observed state; intended-state changes are source repairs. Prefer a
  narrow `SetField`; `ReconcileFromSource` restores a whole object. After a workload write, append
  `program.StalledRolloutRelease(v, ref)`.
- `RunScript`: inline bash byte-identical to the recorded command, application inputs only through
  relation-derived Env, and digest-to-receipt linkage in `script_origins`.
- Credentials stay references (env or ConfigMap key); never read Secret data.
- When the check reports that no finding binds a program's roles (`reflection.detector-required`), add an
  incident detector with `sdo-detector-authoring`.

## Corpus labels

A case is `.sdo/corpus/<case-id>/`. Use a new ID; never edit or delete an accepted case.

- `labels.json`:
  - `root_cause_program_ids`: programs whose Diagnose must Match `open.json` (the fault).
  - `no_match_program_ids`: programs that must NoMatch it (a healthy state, or a fixed capture a program
    wrongly matched).
  - `bindings.objects`: role name (as in `ParamSpec`) to `{api_version, kind, namespace, name}`; bind each
    resource the linked receipts repaired, or its recorded owner.
  - `provenance.incident_id`: this incident. `script_origins`: RunScript sha256 to receipt ID.
- `recordings.json`: the replies Diagnose reads, `{"phase", "method", "arguments", "result"}` (empty phase
  is open; `fixed`, `healthy`), plus one `RepairReceipt` entry per encoded hand repair (`create_corpus` adds them; a
  `reflection.program-required` hint prints the exact entry otherwise).
- `open.json`, `fixed.json`, `healthy.json` snapshots, and `accepted.json` when Mitigate reads source.
  Missing captures never block saving; replay results are information.

## More detail, only when needed

- A diagnostic names a rule: [validator rules](references/rules.md).
- Patches, commands, HTTP, Jobs: [general actions](references/general-actions.md); typed API reads:
  [recorded client](references/kubernetes-client.md).
- Stalled rollouts, database catalogs, routing probes, Service ports, per-cause work:
  [program notes](references/programs.md).
- Full generated API: [program SDK](references/program-sdk.md).
