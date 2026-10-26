---
name: sdo-incident-response
description: Resolve incidents with coverage, evidence and independent verification.
---

# Incident response

<<<<<<< HEAD
Accepted source and developer changes express intent. Operational memory is fallible advice. Recovery requires evidence from the intended application behavior. Discover capabilities and argument shapes in [commands](references/commands.md). Use jq for JSON. [Credentials](references/database-credentials.md).

1. Read `playbook_coverage`: matches, bindings, capability, unexplained findings. Success and failure counts are information, never permission gates. A diagnosis-only match supplies evidence, not a repair. Check current status and any late findings; use a quick live check of their preconditions and accepted-source values. Investigate contradictions.
2. Apply fixes or several independent mitigations together; check SDO's verification and continue until it verifies. Preserve partial repairs; treat new findings as possible regressions.
3. Use recorded actions for live mutations and declare every changed resource; never hand-write receipts. Commit source repairs. Restore namespace drift to accepted source unless live evidence establishes an intended difference; uncertainty alone does not block restoration. Explain benign drift with live evidence. Menu offers repairs and source-absent removal. Edit playbooks, run and report changes. Scripts are learnable mitigations with relation-derived Values within incident budgets.
4. SDO's independent verification is the source of truth; wait on it instead of sleeping. Action success is not recovery. Pending traffic is unproven; its reason names unproven scenarios. Verify after fresh observations. Report repaired causes promptly, including late faults and affected resources. Cite live cause observations, explained_detectors and verification results. Always verify; never claim done without this self-check. If findings clear without a repair, return cancelled rather than inventing one.
5. Controller progress, even empty snapshots, is authoritative: summaries flag possible regressions and fit stdout with JSON framing. The complete JSON report holds evidence/errors and schema. The incident-verified warning is informational: reads/reports stay available; closed-incident mutations are rejected. Report grace, outcomes and reflection continue in maintenance.
6. Undo only proven wrong or harmful changes; keep useful repairs while findings persist. Clean up temporary investigation resources; label helper Pods or Jobs `sdo.dev/responder-helper=true`, never application workloads.
7. On re-prompt, work only `still_broken`. Preserve repairs in `already_done`, then verify again. The controller independently checks the result.

Use checkout-relative paths and scratch outside memory. Read complete output files, never truncated prefixes. Let blocking operations finish in one call instead of polling across model turns. Static files belong in static_context, not live cause evidence.
=======
Accepted source and developer changes express intent. Operational memory is fallible advice. Recovery requires evidence from the intended application behavior. Discover capabilities and argument shapes in [commands](references/commands.md); help. Use jq. [Credentials](references/database-credentials.md).

1. Read `playbook_coverage`: matches/bindings, can_mitigate, unexplained findings. Success and failure counts are information, never permission gates. A diagnosis-only match supplies evidence, not a repair. Check current status and any late findings; use a quick live check of their preconditions and accepted-source values. Investigate contradictions.
2. Apply fixes or several independent mitigations together; check SDO's verification and continue until it verifies. Preserve partial repairs; new findings may be regressions.
3. Use recorded actions for live mutations and declare every changed resource; never hand-write receipts. Commit source repairs. Restore namespace drift to accepted source unless live evidence establishes an intended difference; uncertainty alone does not block restoration. Explain benign drift with live evidence. Menu offers repairs and source-absent removal. Edit playbooks in place; run and report changes. Script fixes are learnable; use relation-derived Values within incident budgets.
4. SDO's independent verification is the source of truth; wait on it instead of sleeping. Action success is not recovery. Pending traffic is unproven; reasons name scenarios. Verify fresh observations. Cite live cause observations, explained_detectors and verification results. Verify; never claim done without this self-check. If findings clear without a repair, return cancelled rather than inventing one.
5. Controller progress, even empty snapshots, is authoritative: summaries flag possible regressions within framed stdout limits. The complete JSON report holds evidence/errors and schema. Incident-verified warning is informational; reads/reports remain; closed-incident mutations are rejected. Report grace/outcomes/reflection continue in maintenance.
6. Undo only proven wrong or harmful changes; retain useful repairs. Clean up temporary investigation resources; label helper Pods or Jobs `sdo.dev/responder-helper=true`, never application workloads.
7. On re-prompt, work only `still_broken`. Preserve repairs in `already_done`, then verify again. The controller independently checks the result.

Prioritize faults behind current user-facing failures; defer harmless cleanup.
Report unfinished work only with a concrete blocker or exhausted repair options.

Use checkout-relative paths and scratch outside memory. Query large output files; never read truncated prefixes. Let blocking operations finish in one call instead of polling across model turns. Static files belong in static_context, not live cause evidence.
>>>>>>> origin/vic-lsh/dev/sdo-improve
