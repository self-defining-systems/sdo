---
name: sdo-incident-response
description: Resolve incidents with coverage, evidence and independent verification.
---

# Incident response

Accepted source and developer changes express intent. Operational memory is fallible advice. Recovery requires evidence from the intended application behavior. Discover capabilities and argument shapes in [commands](references/commands.md); [Credentials](references/database-credentials.md).

1. Read `playbook_coverage`: matches, bindings, mitigation capability, unexplained findings. Success and failure counts are information, never permission gates. A diagnosis-only match supplies evidence, not a repair. Check current status and any late findings; use a quick live check of their preconditions and accepted-source values. Investigate contradictions.
2. Apply fixes or several independent mitigations together; check SDO's verification and continue until it verifies. Preserve partial repairs; treat new findings as possible regressions.
3. Use recorded actions for live mutations and declare every changed resource; never hand-write receipts. Commit needed source repairs. Restore namespace drift to accepted source unless live evidence establishes an intended difference; uncertainty alone does not block restoration. Explain benign drift with live evidence. Menu: repairs and source-absent object removal. Keep memory read-only; report logical playbook IDs. Script fixes are first-class learnable mitigations with relation-derived Values, within incident budgets.
4. SDO's independent verification is the source of truth; wait on it instead of sleeping. Action success is not recovery. Pending traffic is unproven; its reason names unproven scenarios. Verify after fresh observations. Cite live cause observations, explained_detectors and verification results. Always verify; never claim done without this self-check. If findings clear without a repair, return cancelled rather than inventing one.
5. Controller progress, even empty snapshots, is authoritative: summaries flag possible regressions and fit stdout's byte limit including JSON framing. The complete JSON report holds evidence/errors and schema. The incident-verified warning is informational: reads/reports stay available; closed-incident mutations are rejected. Report grace, outcomes and reflection continue in maintenance.
6. Undo only proven wrong or harmful changes; keep useful repairs while findings persist. Clean up temporary investigation resources; label helper Pods or Jobs `sdo.dev/responder-helper=true`, never application workloads.
7. On re-prompt, work only `still_broken`. Preserve repairs in `already_done`, then verify again. The controller independently checks the result.

Use checkout-relative paths and scratch outside memory. Extract large output from files; never read truncated prefixes. Let blocking operations finish in one call instead of polling across model turns. Static files belong in static_context, not live cause evidence.
