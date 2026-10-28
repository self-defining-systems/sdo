---
name: sdo-memory
description: Find and use operational memory by logical ID, or validate authorized memory edits. Covers artifact ownership, accepted provenance, and the worker's typed authoring interface.
---

# Operational memory

Memory exists so later incidents are fixed faster and cheaper. Read and reason across all accepted memory; incident-linked entries come first and full files remain readable in the checkout. Edit only the artifact classes and proposal checkout your task authorizes.

1. Read the supplied playbook index (ID and one-line description), then open only relevant READMEs; read a playbook before using it. Check its diagnosis against current resources first, or after applying when the match is ready to mitigate.
2. For reflection, follow [`sdo-playbook-authoring`](../sdo-playbook-authoring/SKILL.md): it is the reflection entry point. For detector changes, read [`sdo-detector-authoring`](../sdo-detector-authoring/SKILL.md).
3. The [generated layout and ownership map](references/layout.md) lists every `.sdo/` path and its owner; preserve accepted provenance and corpus cases, and the task's actor, phase, accepted base and incident identity.
4. Validate final edits with [check_memory](references/runtime.md#check_memory), supply typed decision fields directly, and return the same decision and logical IDs; correct named diagnostics before checking again. The broker independently validates final bytes.

## Memory principles

- Treat memory as advice and accepted source as intent; check both against live state before acting.
- Keep one failure mechanism per playbook; refine it when only a resource or parameter differs.
- Avoid hard-coded service or app names: prefer runtime bindings from findings, Diagnose or responder values; isolate unavoidable names in one logical helper and document app constants in the playbook README.
- Use detectors to find problems proactively and playbooks to diagnose and mitigate; their relationship need not be one to one.
- Refine a playbook whose mitigation failed or was undone, or set `status: retired` and a nonempty `retired_reason` in its front matter; retired playbooks remain in history but are not compiled or offered.
- Keep memory small and current: give each playbook README a one-line `description:` in its YAML front matter, and merge duplicates.
- Preserve [credential references](../sdo-incident-response/references/database-credentials.md), never resolved values.
- Exclude benchmark/grader details, secrets and physical paths from memory text; prose and results use logical IDs.

The [generated runtime contract](references/runtime.md) defines decision fields, envelope shape, tool names and capture phases. Read [validator rules](references/rules.md) when a diagnostic names an ownership, layout or schema rule.
