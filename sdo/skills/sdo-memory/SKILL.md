---
name: sdo-memory
description: Find and use operational memory by logical ID, or validate authorized memory edits. Covers artifact ownership, accepted provenance, and the worker's typed authoring interface.
---

# Operational memory

Memory exists so later incidents are fixed faster and cheaper. Read and reason across all accepted memory; incident-linked entries come first and full files remain readable in the checkout. Edit only the artifact classes and proposal checkout your task authorizes.

1. Read the supplied playbook index (ID and one-line description), then open only relevant READMEs; read a playbook before using it. Check its diagnosis against current resources first, or after applying when the match is ready to mitigate.
2. Before editing, read the [generated layout and ownership map](references/layout.md); preserve accepted provenance and corpus cases, and the task's actor, phase, accepted base and incident identity.
3. For reflection, read [`sdo-playbook-authoring`](../sdo-playbook-authoring/SKILL.md); for detector changes, read [`sdo-detector-authoring`](../sdo-detector-authoring/SKILL.md). Open only the reference needed for the current step.
4. Validate final edits with [check_memory](references/runtime.md#check_memory), supply typed decision fields directly, and return the same decision and logical IDs; correct named diagnostics before checking again. The broker independently validates final bytes.

## Memory principles

- Treat memory as advice and accepted source as intent; check both against live state before acting.
- First judge whether accepted memory already covers the incident, including applied playbooks, matching detectors and unchanged source. Only when existing playbooks match well may you return `no_change` with a short reason and finish; cover every recorded hand repair with an executable typed SDK or `RunScript` step, and update or retire applied playbooks that failed, left findings unresolved or needed later hand repair. Otherwise make the smallest useful addition.
- Save new verified steps even with partial coverage; replay results are information, never a save gate. Any performed repair can be a `RunScript` bash step with relation-derived Env and digest-to-receipt linkage. The README explains judgement about when and in what order to run steps, and what to observe and decide. README text carries only judgement and ordering. Link each recorded repair to its executable step using the original receipt, even when replay is unavailable. Return `updated` with program IDs; typed `sdk_gaps` name actually unavailable observations or effects, never work not encoded yet.
- Keep one failure mechanism per playbook; refine it when only a resource or parameter differs.
- Reflection avoids hard-coded service or app names: prefer runtime bindings from findings, Diagnose or responder values; isolate unavoidable names in one logical helper and document app constants in the playbook README.
- Use detectors to find problems proactively and playbooks to diagnose and mitigate; their relationship need not be one to one.
- Refine a playbook whose mitigation failed or was undone, or set `status: retired` and a nonempty `retired_reason` in its front matter; retired playbooks remain in history but are not compiled or offered.
- Keep memory small and current: give each playbook README a one-line `description:` in its YAML front matter; maintain its index and merge duplicates.
- A script fix is a first-class learnable mitigation with relation-derived Values, within incident budgets.
- Preserve [credential references](../sdo-incident-response/references/database-credentials.md), never resolved values: typed database repairs accept env or namespace-local ConfigMap keys and optional literal assignment names; only the executor resolves them.
- Exclude benchmark/grader details, secrets and physical paths from memory text; physical layout is only for checkout edits and tooling, while prose and results use logical IDs.

The [generated runtime contract](references/runtime.md) defines decision fields, envelope shape, tool names and capture phases. The final answer uses the session envelope; the CLI decision file contains only its reflection result. The typed validation tool handles the distinction.

Before returning memory edits, run the complete memory check and resolve every diagnostic until it passes.

Read [validator rules](references/rules.md) when a diagnostic names an ownership, layout or schema rule.
