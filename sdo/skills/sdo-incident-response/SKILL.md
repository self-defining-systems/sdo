---
name: sdo-incident-response
description: Resolve incidents with coverage, evidence and independent verification.
---

# Incident response

Accepted source and developer changes express intent. Operational memory is fallible advice. Recovery requires evidence from the intended application behavior. Discover capabilities and argument shapes in [commands](references/commands.md). Use jq. [Credentials](references/database-credentials.md).

1. Read `playbook_coverage`: matches/bindings, can_mitigate, unexplained findings. Counts inform; they never gate permission. Diagnosis-only matches supply evidence. Check status and late findings; use a quick live check of their preconditions and accepted-source values. Investigate contradictions.
2. Apply fixes or several independent mitigations together; check SDO's verification and continue until it verifies. Preserve partial repairs; new findings may be regressions.
3. Use recorded actions for live mutations and declare every changed resource; never hand-write receipts. API reads follow client-go. Commit source repairs. Restore namespace drift to accepted source unless live evidence establishes an intended difference; uncertainty alone does not block restoration. Explain benign drift with live evidence. Menu: repairs/source-absent removal. Edit playbooks, run and report changes. Script fixes are learnable; use relation-derived Values within budgets.
4. SDO's independent verification is the source of truth; wait on it instead of sleeping. Action success is not recovery. Pending traffic is unproven; reasons name scenarios. Verify fresh observations. Report repaired causes promptly, including late faults. Cite live cause observations, explained_detectors and verification results. Verify; never claim done without this self-check. If findings clear without a repair, return cancelled rather than inventing one.
5. Controller progress, even empty snapshots, is authoritative: summaries flag possible regressions within stdout limits. The complete JSON report holds evidence/errors and schema. Incident-verified warning is informational; reads/reports remain; closed-incident mutations are rejected. Report grace/outcomes/reflection continue in maintenance.
6. Undo only proven wrong or harmful changes; retain useful repairs. Clean up temporary investigation resources; label helper Pods or Jobs `sdo.dev/responder-helper=true`, never application workloads.
7. Fix `still_broken`; keep `already_done`. Merge accepted peer learning on memory conflicts.

Read a typed action's failure stage and fix source inputs before raw commands.

Prioritize current user-facing failures; defer harmless cleanup. Report unfinished work only with a blocker or exhausted options.

Use checkout-relative paths and scratch outside memory. Read complete output files, never prefixes. Let blocking operations finish in one call instead of polling across model turns. Static files belong in static_context, not live cause evidence.
