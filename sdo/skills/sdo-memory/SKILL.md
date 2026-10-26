---
name: sdo-memory
description: Find and use operational memory by logical ID, or validate authorized memory edits. Covers artifact ownership, accepted provenance, and the worker's typed authoring interface.
---

# Operational memory

Read memory freely; edit only the artifact classes and proposal checkout your task authorizes.

1. Find the matching logical IDs in the supplied snapshot or inventory; read a playbook before using it and check its diagnosis against current resources.
2. Before editing, read the [generated layout and ownership map](references/layout.md); preserve accepted provenance and corpus cases, and the task's actor, phase, accepted base and incident identity.
3. For reflection, read the installed `sdo-playbook-authoring` skill; for detector changes, read `sdo-detector-authoring`. Open only the reference needed for the current step.
4. Validate final edits with [check_memory](references/runtime.md#check_memory), supply typed decision fields directly, and return the same decision and logical IDs; correct named diagnostics before checking again. The broker independently validates final bytes.

## Memory principles

- Treat memory as advice and accepted source as intent; check both against live state before acting.
- Learn only from independently verified evidence; a README alone does not establish learning.
- Keep one failure mechanism per playbook; refine it when only a resource or parameter differs.
- Reflection avoids hard-coded service or app names: prefer runtime bindings from findings, Diagnose or responder values; isolate unavoidable names in one logical helper and document app constants in the playbook README.
- Use detectors to find problems proactively and playbooks to diagnose and mitigate; their relationship need not be one to one.
- Refine a playbook whose mitigation failed or was undone, or set `status: retired` and a nonempty `retired_reason` in its front matter; retired playbooks remain in history but are not compiled or offered.
- Keep memory small and current: maintain its index, merge duplicates and respect the worker snapshot budget.
- A script fix is a first-class learnable mitigation with relation-derived Values, within incident budgets.
- Preserve [credential references](../sdo-incident-response/references/database-credentials.md), never resolved values: typed database repairs accept env or namespace-local ConfigMap keys and optional literal assignment names; only the executor resolves them.
- Exclude benchmark/grader details, secrets and physical paths from memory text; physical layout is only for checkout edits and tooling, while prose and results use logical IDs.

The [generated runtime contract](references/runtime.md) defines decision fields, envelope shape, tool names and capture phases. The final answer uses the session envelope; the CLI decision file contains only its reflection result. The typed validation tool handles the distinction.

Before returning memory edits, run the complete memory check and resolve every diagnostic until it passes.

Read [validator rules](references/rules.md) when a diagnostic names an ownership, layout or schema rule.
