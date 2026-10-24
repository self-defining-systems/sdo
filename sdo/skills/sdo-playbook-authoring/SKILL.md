---
name: sdo-playbook-authoring
description: Learn or refine reusable playbooks and deterministic Go programs from independently verified incident outcomes. Covers source grounding, captured evidence, parameterized diagnosis and replay.
---

# Authoring a playbook

A playbook captures a verified failure mechanism and repair for reuse on different resources.
Accepted Mitigators become responder menu options, a convenience over direct commands through the ordinary
executor. Declare object and value parameters, group actions that must run together, and supply specific
failure context for the report returned to the same responder session.
The installed `sdo-memory` skill defines ownership, logical IDs and the generated decision/tool contract.

1. Compare the confirmed mechanism and repair with accepted playbooks. Refine one when only identity or a
   parameter differs; create one only for a different mechanism or repair.
2. For deterministic programs, read [program authoring](references/programs.md) before editing and the
   [generated program SDK](references/program-sdk.md) before choosing API types. The
   [authoring_reference](../sdo-memory/references/runtime.md#authoring_reference) tool retrieves these same
   shipped references without searching installation directories.
3. For each independently verified cause, submit a diagnose program, mitigation program, a README explaining
   how to handle the fault, and detector coverage. A detector may surface multiple playbooks.
   Use the responder's recorded actions and existing session observations to build the replay fixture.
   Controller captures and the evidence tools are optional sources, never prerequisites for learning.
4. Author final files in the proposal checkout and register every playbook in its index. A fixture labels
   its positive programs and binds the confirmed-cause resources. Include observed primitive replies and
   accepted.json when the mitigation needs source values. Preserve accepted cases; add new evidence by ID.
5. Validate compilation and mitigation-clears-diagnosis replay, then return the same decision and changed
   logical IDs. A README-only update fails for a verified repair. The only exception is a typed SDK gap
   naming the cause resources, missing primitive and concrete reason; it cannot excuse other causes.

Use role placeholders in prose and bound resources in Go. Mitigation returns to accepted or observed state;
intended-state changes are source repairs. An emergency literal is a declared parameter the responder fills
and justifies.
Read [validator rules](references/rules.md) when a diagnostic names a playbook, script or corpus rule.
