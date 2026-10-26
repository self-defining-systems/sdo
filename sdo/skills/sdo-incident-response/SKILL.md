---
name: sdo-incident-response
description: Resolve incidents with coverage, evidence and independent verification.
---

# Incident response

Accepted source and developer changes express intent; memory is fallible advice. Recovery needs evidence of intended behavior. Discover capabilities and shapes in [commands](references/commands.md) and help. Use jq for JSON. [Credentials](references/database-credentials.md).

1. Read `playbook_coverage`: matches, bindings, mitigation capability, unexplained findings. Success/failure counts are information, never permission gates. Diagnosis-only matches supply evidence. Check status, late findings, live preconditions and accepted-source values; investigate contradictions.
2. Apply fixes or several independent mitigations together; check SDO's verification and continue until it verifies. Preserve partial repairs; treat new findings as possible regressions.
3. Use recorded actions for live mutations and declare every changed resource; never hand-write receipts. Commit needed source repairs. Restore namespace drift to accepted source unless live evidence establishes an intended difference; uncertainty alone does not block restoration. Explain benign drift with live evidence. Menu: repairs and source-absent object removal. Playbooks are editable code: adapt in place, run, report changes. Script fixes are learnable mitigations with relation-derived Values within incident budgets.
4. SDO's independent verification is the source of truth; wait on it instead of sleeping. Action success is not recovery. Pending traffic is unproven; its reason names unproven scenarios. Verify after fresh observations. Cite live cause observations, explained_detectors and verification results. Always verify; never claim done without this self-check. If findings clear without a repair, return cancelled rather than inventing one.
5. Controller progress, even empty snapshots, is authoritative: summaries flag possible regressions within stdout's framed byte limit. The complete JSON report holds evidence/errors and schema. The incident-verified warning is informational: reads/reports stay available; closed-incident mutations are rejected. Report grace, outcomes and reflection continue in maintenance.
6. Undo only proven wrong or harmful changes; retain useful repairs. Clean up temporary investigation resources; label helper Pods or Jobs `sdo.dev/responder-helper=true`, never application workloads.
7. On re-prompt, work only `still_broken`. Preserve repairs in `already_done`, then verify again. The controller independently checks the result.

Prioritize faults behind current user-facing failures; defer harmless cleanup.
Report unfinished work only with a concrete blocker or exhausted repair options.

Use checkout-relative paths and scratch outside memory. Extract large output from files; never read truncated prefixes. Complete blocking operations in one call. Static files belong in static_context, not live cause evidence.
