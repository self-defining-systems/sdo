---
name: sdo-playbook-authoring
description: Learn or refine reusable playbooks and deterministic Go programs from independently verified incident outcomes. Covers source grounding, captured evidence, parameterized diagnosis and replay.
---

# Authoring a playbook

A playbook captures a verified failure mechanism and repair for reuse on different resources.
The installed `sdo-memory` skill defines ownership, logical IDs and the generated decision/tool contract.

1. Compare the confirmed mechanism and repair with accepted playbooks. Refine one when only identity or a
   parameter differs; create one only for a different mechanism or repair.
2. For deterministic programs, read [program authoring](references/programs.md) before editing and the
   [generated program SDK](references/program-sdk.md) before choosing API types. The
   [authoring_reference](../sdo-memory/references/runtime.md#authoring_reference) tool retrieves these same
   shipped references without searching installation directories.
3. Ground positive cases in controller captures. Use
   [create_corpus](../sdo-memory/references/runtime.md#create_corpus) to preserve captured bytes and supply
   provenance. Inspect available evidence before deciding a cause is learnable; later live observations do
   not establish what an earlier capture contained.
4. Author final files in the proposal checkout and register every playbook in its index. Keep program README
   prose to judgement and context; keep scripts out of program directories. Prose-only playbooks may use
   named, executable scripts for repeatable diagnosis or repair. The
   [restart-loop example](assets/restart-loop/README.md) illustrates a prose playbook.
5. Validate with the typed memory check, then return the same learning decision and every changed logical ID.
   Report an evidence or SDK gap when no captured case supports a safe reusable relation.

Use role placeholders in prose, bound resources in Go, and accepted source for desired state.
Read [validator rules](references/rules.md) when a diagnostic names a playbook, script or corpus rule.
