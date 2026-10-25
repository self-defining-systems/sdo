---
name: sdo-incident-response
description: Resolve incidents using coverage, evidence and independent verification.
---

# Incident response

Accepted source and developer changes express intent. Operational memory is fallible advice. Recovery requires evidence from the intended application behavior. Discover capabilities and argument shapes through local tool help.

1. Read `playbook_coverage`: matches, bindings, mitigation capability and unexplained findings. Success and failure counts are information, never permission gates. A diagnosis-only match supplies evidence, not a repair. Check current status and any late findings; use a quick live check of their preconditions and accepted-source values. Investigate contradictions.
2. Work in a loop: apply a fix, or several independent mitigations together, check SDO's verification, and continue until it verifies; memory speeds fixes without replacing checks. Preserve partial repairs, continue while nothing regresses, and treat new findings as possible regressions.
3. Use recorded actions for live mutations and declare every changed resource; never hand-write receipts. Commit source repairs when needed. Restore namespace drift to accepted source unless live evidence establishes an intended difference; uncertainty alone does not block restoration. Explain benign drift with live evidence. The menu includes repairs and removal of source-absent objects. Keep operational memory read-only and report playbooks by logical ID.
4. SDO's independent verification is the source of truth; wait on it instead of sleeping. Action success does not establish incident recovery. Cite live cause observations, explained_detectors and verification results. Never skip verification: never claim done without this self-check. If findings clear without a repair, return cancelled rather than inventing one.
5. Controller progress, including empty snapshots, is authoritative: mutation summaries flag possible regressions and fit the stdout byte limit including JSON framing; the complete JSON report holds evidence/errors and schema. The incident-verified warning is informational: read/report results remain available, while closed-incident mutations are rejected.
6. Undo only changes shown wrong or harmful; preserve useful repairs while other findings persist. Clean up temporary investigation resources and label helper Pods or Jobs `sdo.dev/responder-helper=true`, never application workloads.
7. On a re-prompt, work only the `still_broken` list. Read `already_done` to preserve completed repairs, then verify again. The controller independently checks the result.

Keep scratch outside operational memory. Use the current checkout and relative paths; never carry absolute paths across turns. Write large tool output to a temporary file and grep or extract from it; never read a truncated prefix. Let blocking operations finish in one call instead of polling across model turns. Static files belong in static_context, not live cause evidence.
