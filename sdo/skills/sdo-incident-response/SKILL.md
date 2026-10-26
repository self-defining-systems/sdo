---
name: sdo-incident-response
description: Resolve incidents.
---

Accepted source and developer changes express intent. Operational memory is fallible advice. Recovery requires evidence from the intended application behavior. Discover capabilities and argument shapes in [commands](references/commands.md). Use jq. [Credentials](references/database-credentials.md).

1. Read `playbook_coverage`: matches, bindings, mitigation, unexplained findings. Success and failure counts are information, never permission gates. A diagnosis-only match supplies evidence, not a repair. Check current status and any late findings; quick live check of their preconditions and accepted-source values. Investigate contradictions.
2. Apply several independent mitigations together; continue until it verifies. Keep partial repairs; new findings may regress.
3. Use recorded actions for live mutations (declare every changed resource); never hand-write receipts. One call per wrapper. API reads follow client-go. Commit source repairs. Restore namespace drift to accepted source unless live evidence establishes an intended difference; uncertainty alone does not block restoration. Explain benign drift with live evidence. Menu IDs persist. When an admission control blocks recovery and its backend is gone, restore the backend if you can. Otherwise a scoped fail-open or removal that restores availability is acceptable; report it. Test/report playbooks. Budget script Values by relation.
4. SDO's independent verification is the source of truth; wait on it instead of sleeping. Inspect rollout blockers and old-template Pods before repeating writes. Action success is not recovery. Traffic is unproven; reasons name scenarios. Verify fresh state. Report repaired causes, including late faults. Cite live cause observations, explained_detectors, verification; never claim done without this self-check. If findings clear without a repair, return cancelled rather than inventing one.
5. Controller progress, even empty snapshots, governs; summaries flag possible regressions; bound stdout. The complete JSON report: evidence/errors/schema. Verified warning is informational; reads/reports remain; closed-incident mutations are rejected. Grace/reflection run in maintenance.
6. Undo proven harms; retain useful repairs. Clean up temporary investigation resources; label helper Pods or Jobs `sdo.dev/responder-helper=true`, never application workloads.
7. Fix `still_broken`; keep `already_done`. The controller independently checks the result. Merge accepted peer learning on conflicts.

Read typed action failure stages; fix source inputs before raw commands.

Prioritize current user-facing failures; defer harmless cleanup. Unfinished work needs a blocker or exhausted options.

Refs: printed JSON or Kind/[namespace/]name.

Use relative paths; scratch outside memory. Read full files. Calls finish in one call instead of polling across model turns. Static files belong in static_context, not live cause evidence.
