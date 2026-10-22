---
name: sdo-memory
description: Find and use operational memory by logical ID, or validate authorized memory edits. Covers artifact ownership, accepted provenance, and the worker's typed authoring interface.
---

# Operational memory

Memory records the health goal, accepted architecture, reusable playbooks and diagnostics, and controller outcomes.
Read it freely; edit only the artifact classes and proposal checkout your task authorizes.

1. Identify artifacts by logical ID from the supplied memory snapshot or memory inventory. Read the matching
   playbook before applying it and confirm its diagnosis against current resources.
2. Bind resource roles from the finding. Authored prose and results use logical IDs and role placeholders;
   physical storage paths belong only to checkout editing and tooling.
3. For authorized reflection, start with the installed `sdo-playbook-authoring` skill; for detector changes,
   start with `sdo-detector-authoring`. Read only the reference needed for the current step.
4. Validate final edits with [check_memory](references/runtime.md#check_memory). Supply typed decision fields
   directly and return the same decision and logical IDs. Correct named diagnostics before checking again.
   The broker independently validates the final bytes before acceptance.

The human owns the goal, the deployer owns architecture, the health judge owns health detectors, the responder
owns incident detectors and playbooks, and the controller owns outcomes. Preserve accepted provenance and
accepted corpus cases. The task supplies actor, phase, accepted base and incident identity; never replace them.

The [generated runtime contract](references/runtime.md) defines decision fields, envelope shape, tool names,
and capture phase values. The model's final answer uses the session envelope; the decision file used by the
CLI contains only its reflection result. The typed validation tool handles that distinction.
Read [validator rules](references/rules.md) when a diagnostic names an ownership, layout or schema rule.
