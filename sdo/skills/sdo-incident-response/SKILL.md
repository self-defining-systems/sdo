---
name: sdo-incident-response
description: Resolve incidents with coverage, evidence and independent verification.
---

# Incident response

Accepted source and developer changes express intent. Operational memory is fallible advice. Recovery requires evidence from the intended application behavior. Discover capabilities and argument shapes in [commands](references/commands.md). Use jq. [Credentials](references/database-credentials.md).

1. Read `playbook_coverage`: matches/bindings, can_mitigate, unexplained findings. Success and failure counts are information, never permission gates. A diagnosis-only match supplies evidence, not a repair. Check current status and any late findings; use a quick live check of their preconditions and accepted-source values. Investigate contradictions.
2. Apply several independent mitigations together; continue until it verifies. Preserve partial repairs.
3. Use recorded actions for live mutations and declare every changed resource; never hand-write receipts. API reads follow client-go. Commit source repairs. Restore namespace drift to accepted source unless live evidence establishes an intended difference; uncertainty alone does not block restoration. Explain benign drift with live evidence. Menu IDs persist. When an admission control blocks recovery and its backend is gone, restore the backend if you can. Otherwise a scoped fail-open or removal that restores availability is acceptable; report it. Test/report playbook edits. Learn script fixes with budgeted relation-derived Values.
4. SDO's independent verification is the source of truth; wait on it instead of sleeping. Inspect rollout blockers and old-template Pods before repeating writes. Action success is not recovery. Pending traffic is unproven; reasons name scenarios. Verify fresh observations. Report all repaired causes promptly. Cite live cause observations, explained_detectors, verification results; never claim done without this self-check. If findings clear without a repair, return cancelled rather than inventing one.
5. Controller progress is authoritative; summaries flag possible regressions. The complete JSON report holds evidence and schema. Verified warning is informational; reads/reports remain; closed-incident mutations are rejected. Reports/outcomes/reflection continue in maintenance.
6. Undo only proven wrong or harmful changes; retain useful repairs. Clean up temporary investigation resources; label helper Pods or Jobs `sdo.dev/responder-helper=true`, never application workloads.
7. Fix `still_broken`; keep `already_done`. The controller independently checks the result. Merge accepted peer learning on memory conflicts.

Prioritize user-facing failures. Report unfinished work only with a blocker or exhausted options.

Use relative paths; keep scratch outside memory. Read complete output files. Operations finish in one call instead of polling across model turns. Static files belong in static_context, not live cause evidence.
