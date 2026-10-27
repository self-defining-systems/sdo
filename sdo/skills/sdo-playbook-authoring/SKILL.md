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

1. Compare the confirmed mechanism and repair with accepted playbooks using the memory principles.
2. For deterministic programs, read [program authoring](references/programs.md) before editing and the
   [generated program SDK](references/program-sdk.md) before choosing API types. The
   [authoring_reference](../sdo-memory/references/runtime.md#authoring_reference) tool retrieves these same
   shipped references as a section index or one section, with a file to search.
3. For each cause a recorded repair touched on a closure verified healthy, submit a diagnose program, mitigation program, a README explaining
   how to handle the fault, and detector coverage. A detector may surface multiple playbooks;
   `DetectionContext.DiffAgainstSource` observes field drift and source absence before playbook selection.
   Use the responder's recorded actions and existing session observations to build the replay fixture.
   Controller captures and the evidence tools are optional sources, never prerequisites for learning.
4. Author final files in the proposal checkout; SDO derives `fault_class` and the index entry. A fixture labels
   its positive programs and binds the confirmed-cause resources. Include observed primitive replies and
   accepted.json when the mitigation needs source values. Preserve accepted cases; add new evidence by ID.
5. Validate compilation, detector tests and mitigation-clears-diagnosis replay, then return the same decision and
   changed logical IDs. A cause you cannot save as a program is recorded as a coverage gap; say why in the result.
   Return `updated` with changed playbook IDs and scoped `sdk_gaps` when only some causes are learnable.
   Controller-listed `undetected_causes` permit new detector coverage for measured repair transitions;
   bind the replay case to their captured open/fixed configuration without claiming independent health proof.

Mitigation returns to accepted or observed state;
intended-state changes are source repairs. An emergency literal is a declared parameter the responder fills
and justifies.

Before returning memory edits, run the complete memory check and resolve every diagnostic until it passes.

Read [validator rules](references/rules.md) when a diagnostic names a playbook, script or corpus rule.

For API reads, use the [recorded Kubernetes client](references/kubernetes-client.md) with upstream client-go conventions.

For object, command, HTTP, Job or subresource repairs, read the [general action vocabulary](references/general-actions.md).
