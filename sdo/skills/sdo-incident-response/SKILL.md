---
name: sdo-incident-response
description: Resolve incidents with coverage, evidence and independent verification.
---

# Incident response

Accepted source and developer changes express intent. Operational memory is fallible advice. Verify intended application behavior. Capabilities and arguments: [commands](references/commands.md). Use jq. [Credentials](references/database-credentials.md).

1. Read `playbook_coverage`: matches/bindings, can_mitigate, unexplained findings. Counts inform, never authorize. A diagnosis-only match supplies evidence, not a repair. Check current/late findings, live preconditions and accepted-source values; investigate contradictions.
2. Apply independent fixes together; continue until SDO verifies recovery. Preserve partial repairs; new findings may be regressions.
3. Use recorded actions for live mutations and declare every changed resource; never hand-write receipts. API reads follow client-go. Commit source repairs. Restore namespace drift to accepted source unless live evidence establishes an intended difference; uncertainty alone does not block restoration. Explain benign drift with live evidence. Menu IDs persist; live checks apply. When an admission control blocks recovery and its backend is gone, restore the backend if you can. Otherwise a scoped fail-open or removal that restores availability is acceptable; report it. Edit playbooks, run and report changes. Script fixes are learnable; use relation-derived Values within budgets.
4. Trust independent SDO verification; wait on it instead of sleeping. Inspect rollout blockers and old-template Pods before repeating writes. Action success is not recovery; pending traffic is unproven. Report repaired causes promptly, including late faults. Cite live cause observations, explained_detectors and verification results. Verify before claiming done. If findings clear without a repair, return cancelled rather than inventing one.
5. Controller progress is authoritative: summaries flag possible regressions within stdout limits. The complete JSON report holds evidence/errors and schema. Incident-verified warning is informational; reads/reports remain; closed-incident mutations are rejected. Report grace/outcomes/reflection continue in maintenance.
6. Undo only proven wrong or harmful changes; retain useful repairs. Clean up temporary investigation resources; label helper Pods or Jobs `sdo.dev/responder-helper=true`, never application workloads.
7. Fix `still_broken`; keep `already_done`. Merge accepted peer learning on memory conflicts.

Prioritize current user-facing failures; defer harmless cleanup. Report unfinished work only with a blocker or exhausted options.

Use checkout-relative paths and scratch outside memory. Read complete output files, never prefixes. Let blocking operations finish in one call instead of polling across model turns. Static files belong in static_context, not live cause evidence.
