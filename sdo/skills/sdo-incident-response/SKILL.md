---
name: sdo-incident-response
description: Repair incidents.
---

# Incident response

Accepted source and developer changes express intent. Operational memory is fallible advice. Recovery requires evidence from the intended application behavior. Discover capabilities and argument shapes in [commands](references/commands.md). Use jq. [Credentials](references/database-credentials.md).

1. Read `playbook_coverage`: matches/bindings, can_mitigate, unexplained findings. Success and failure counts are information, never permission gates. A diagnosis-only match supplies evidence, not a repair. Check current status and any late findings; use a quick live check of their preconditions and accepted-source values. Investigate contradictions. Reuse earlier stages' actions and results; don't redo verified repairs.
2. Apply fixes or several independent mitigations together; check SDO's verification and continue until it verifies. Keep partial repairs; new findings may be regressions.
3. Use recorded actions for live mutations and declare every changed resource; never hand-write receipts. API reads follow client-go. Commit source repairs. Restore namespace drift to accepted source unless live evidence establishes an intended difference; uncertainty alone does not block restoration. Explain benign drift with live evidence. Menu IDs persist; recheck live. Edit playbooks, run and report changes. Script fixes are learnable; use relation-derived Values within budgets.
4. SDO's independent verification is the source of truth; wait on it instead of sleeping. Action success is not recovery. Pending traffic is unproven; reasons name scenarios. Verify fresh observations; promptly report repaired causes and late faults. Cite live cause observations, explained_detectors and verification results. Verify; never claim done without this self-check. If findings clear without a repair, return cancelled rather than inventing one.
5. Even empty controller snapshots are authoritative. The complete JSON report has schema, evidence/errors and possible regressions. Incident-verified warning is informational; reads/reports remain; closed-incident mutations are rejected. Grace/outcomes/reflection continue in maintenance.
6. Undo only proven wrong or harmful changes; retain useful repairs. Clean up temporary investigation resources; label helper Pods or Jobs `sdo.dev/responder-helper=true`, never application workloads.
7. Fix `still_broken`; keep `already_done`. The controller independently checks the result. Merge accepted peer learning on conflicts.

Prioritize user-facing failures; defer harmless cleanup. Unfinished work requires a blocker or exhausted options.

Use checkout-relative paths and scratch outside memory. Read complete output files, never prefixes. Let blocking operations finish in one call instead of polling across model turns. Static files belong in static_context, not live cause evidence.

Shell: free-form notes/scripts and script evidence; verify independently.
