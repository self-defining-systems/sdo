---
name: sdo-playbook-authoring
description: Learn or refine reusable playbooks and deterministic Go programs from incident outcomes whose closure verified healthy. Covers source grounding, captured evidence, parameterized diagnosis and replay.
---

# Authoring a playbook

A playbook captures a verified failure mechanism and repair for reuse on different resources.
Accepted Mitigators become responder menu options, a convenience over direct commands through the ordinary
executor. Bind required roles from findings; Diagnose reads objects by relation, `LogsWithOptions` (previous instances), and `PodUsage`/`NodeUsage` through ReadView.
Declare object and value parameters, group actions that must run together, and supply specific
failure context for the report returned to the same responder session.
Read the [memory principles](../sdo-memory/SKILL.md#memory-principles) for evidence, reuse, generalization and retirement; that skill also defines ownership and logical IDs.

1. First judge whether all accepted memory already covers the incident: applied playbooks, matching detectors and unchanged source. Read across unrelated memory as needed. Only when existing playbooks match well may you return `no_change` with a short reason and finish; cover every recorded hand repair with an executable typed SDK or `RunScript` step, and update or retire applied playbooks that failed, left findings unresolved or needed later hand repair; otherwise make the smallest useful addition.
   Keep revisions that applied cleanly unless this incident shows failure or incompleteness; add or narrowly edit, never rewrite working steps.
2. For deterministic programs, read [program authoring](references/programs.md) before editing and the
   [generated program SDK](references/program-sdk.md) before choosing API types. The
   [authoring_reference](../sdo-memory/references/runtime.md#authoring_reference) tool retrieves these same
   shipped references as a section index or one section, with a file to search.
3. Save each verified mechanical step as a diagnose and mitigation program, even with partial coverage.
   `RunScript` bash can express any probe or repair the responder ran: use relation-derived Env and
   digest-to-receipt linkage to its original receipt; replay evidence is optional.
   A program's diagnosis should check the conditions its action needs (for example, the cause is gone before
   a restart), so the automatic stage, which runs programs one at a time with a fresh diagnosis before each,
   can find a working order. Put judgement about when and in
   what order to run steps in the README, with observations and decisions that guide it.
   Stop the ongoing cause or producer before restarting its victims; if the repair cannot, state that limitation in the README.
   README text holds only judgement and ordering; every recorded repair needs an executable step linked to its original receipt. Add detector coverage. A detector may surface multiple playbooks;
   `DetectionContext.DiffAgainstSource` observes field drift and source absence before playbook selection.
   Use the responder's recorded actions and existing session observations to build the replay fixture.
   Controller captures and the evidence tools are optional sources, never prerequisites for learning.
4. Author final files in the proposal checkout; SDO derives `fault_class` and the index entry. A fixture labels
   its positive programs and binds the confirmed-cause resources. Include observed primitive replies and
   accepted.json when the mitigation needs source values. Preserve accepted cases; add new evidence by ID.
5. Run the complete memory check. Compilation, imports, changed-source formatting and receipt provenance
   remain required. Replay runs as information: NoMatch, persistent Match and unavailable observations
   never block saving. Scope diagnosis to the step's resources for useful live decisions.
   Return `updated` with changed playbook IDs. Typed `sdk_gaps` name actually unavailable observations or effects; work not encoded yet is not a gap and cannot excuse a recorded repair.
   Controller-listed `undetected_causes` permit new detector coverage for measured repair transitions;
   bind the replay case to their captured open/fixed configuration without claiming independent health proof.

Mitigation returns to accepted or observed state;
intended-state changes are source repairs. An emergency literal is a declared parameter the responder fills
and justifies.

Before returning memory edits, run the complete memory check and resolve blocking diagnostics; do not correct informational replay results.

Read [validator rules](references/rules.md) when a diagnostic names a playbook, script or corpus rule.

For API reads, use the [recorded Kubernetes client](references/kubernetes-client.md) with upstream client-go conventions.

For object, command, HTTP, Job or subresource repairs, read the [general action vocabulary](references/general-actions.md).
