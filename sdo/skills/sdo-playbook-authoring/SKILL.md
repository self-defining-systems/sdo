---
name: sdo-playbook-authoring
description: Start here for reflection. Turn recorded hand repairs into validated playbook programs (diagnose.go, mitigate.go), or return no_change.
---

# From hand repairs to validated programs

Work in the proposal checkout, in this order. Open other files only when a step or a diagnostic names them.

1. **Add detection.** A new or materially changed playbook with a program ships with a cause-specific incident detector
   in this reflection ([how](../sdo-detector-authoring/references/cause-specific.md)); skip it only when just the generic symptom exists, and say so in the outcome reason. Health detectors stay designer-owned.
   **Ask the check what is left.** Use supplied diagnostics without repeating a rejected probe. Otherwise call `check_memory` with `learning_decision: no_change` and a one-line `no_change_reason`.
   If it passes and no detection gap remains, return that decision immediately. Its diagnostics identify required work: uncovered hand
   repairs (`reflection.program-required`), applied playbooks that failed, left findings unresolved or needed a later hand repair
   (`reflection.stale-playbook`: update, narrow its Diagnose off a cause it does not fix, or retire), no verified repair at all (`reflection.repair-required`: return `no_change`; an escalated closure saves a Diagnose-only playbook, `reflection.escalation-diagnose`), and the decision to return when Go
   already settled evidence in this checkout.
2. **Map each hand repair to a step.** Ledger **all successful hand repairs** in the outcome (`repair_actions`,
   `plan_applications`), not only receipts diagnostics name, including generic menu repairs and dependent Pod
   replacements, apart from accepted playbook applications. Map each receipt and target to a program/action,
   keeping the receipt's API version, Kind and semantics. Each needs an executable step: the typed action that
   ran, or `RunScript` with the recorded bytes; README text and `sdk_gaps` never replace a step (work
   not encoded yet is not an SDK gap). Group by repair method; one program covers every component it repairs.
3. **Copy the closest worked example** from `assets/examples/` (`playbooks/<id>/`, each passing the memory check): `deployment-source-drift`, `webhook-ca-sync`,
   `database-user-recovery`, `crashloop-script-repair` (RunScript). Exact signatures are on the
   [SDK card](references/sdk-card.md), grouped by fault.
4. **Write `.sdo/playbooks/<id>/`**: `diagnose.go` (`Spec`, `Diagnose`), `mitigate.go` (`Mitigate`, same
   package and spec) and `README.md` with front matter `schema_version: 1`, `owner: responder`,
   `originating_incident: <incident id>`, `description: <one line>`. The README holds only judgement and
   ordering: when to use it, what first, what to check after. SDO adds `fault_class` and the index entry.
<!-- replay:on -->
5. **Label one corpus case** per new program (rules below): `create_corpus` copies the controller's captures
   (pass `observations` only when none exist, with `repair_receipt_ids`) and writes it in one call. Each program should Match its fault
   capture and NoMatch healthy: decide from what the capture holds (snapshot objects, `pod_admission`, reads
   `recordings_index` lists); a read the capture lacks replays as Unknown, so keep uncaptured live-only reads
   in Mitigate, as `webhook-ca-sync` does.
<!-- /replay:on -->
6. **Check**: `check_memory` with `learning_decision: updated`, `proposed_artifact_ids` = the changed
   playbook IDs, and a summary. Fix each blocking diagnostic as its hint says; follow `next`. A healthy-replay
   fault the accepted base also has is pre-existing: do not edit it. Six failures end checks.
7. When `next` says return, return the same decision and IDs; do not polish passing memory.

Keep revisions that applied cleanly unless this incident shows failure or incompleteness; add or narrowly edit, never rewrite working steps. Preserve verified partial steps. Separate independent source-repair causes
([program notes](references/programs.md#source-restoration)).

## Program rules

- [One playbook per repair method](references/rules.md#one-playbook-per-repair-method), not per symptom or service.
- Roles come from findings: one `ParamSpec` per object Kind. Read other objects through ReadView by
  relation (selector, owner, mount), never by app name; put unavoidable app facts in the README.
- Write values only as `FromSource`, `FromObserved` or `FromParam` (a declared scalar with a default).
- Diagnose returns Match on positive evidence about the bound component, NoMatch when absent, an error
  (Unknown) when a read is unavailable. A healthy, source-matching object is no evidence. Confirm the fault class on the
  bound target (image, ports, a read or exec); NoMatch on a workload of the wrong kind, with a near-miss test.
- Mitigate returns to accepted or observed state: [more program rules](references/programs.md#more-program-rules).
- `RunScript`: inline bash byte-identical to the recorded command, application inputs only through
  relation-derived Env, and a successful accepted receipt for those bytes; preserve
  [each script revision's original receipt](references/script-receipts.md).
<!-- replay:on -->
  Link the script digest to its receipt ID in the case's `script_origins`.
<!-- /replay:on -->

- `reflection.detector-required`: no finding binds the program's roles. List the playbook in the detector it names, or copy
  `assets/examples/detectors/crashloop-script-repair/` to `.sdo/diagnostics/detectors/incidents/<id>/`,
  keep its manifest and `Spec` in step, and bind the role in `ParameterBindings`.

<!-- replay:on -->
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
  Omit `open.json` or `healthy.json` when identical to `fixed.json`; the loader uses that file.
  Missing captures never block saving; replay results are information. Diagnose replays recorded phase
  reads, including source comparisons, webhooks, database catalogs and pod admission; an unrecorded read
  is Unknown and names the call. Use its open capture (repair reads are in pre-apply); never invent replies.

<!-- /replay:on -->
## More detail, only when needed

- A diagnostic names a rule: [validator rules](references/rules.md).
- Patches, commands, HTTP, Jobs: [general actions](references/general-actions.md); typed API reads:
  [recorded client](references/kubernetes-client.md).
- Stalled rollouts, database catalogs, routing probes, Service ports, per-cause work:
  [program notes](references/programs.md).
- Full generated API: [program SDK](references/program-sdk.md), sections
  [01](references/program-sdk/program-01.md), [02](references/program-sdk/program-02.md),
  [03](references/program-sdk/program-03.md), [04](references/program-sdk/program-04.md),
  [routing](references/program-sdk/routing-01.md).
