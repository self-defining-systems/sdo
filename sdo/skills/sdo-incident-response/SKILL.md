---
name: sdo-incident-response
description: Resolve incidents using learned playbook coverage, live repair evidence, detector self-checks, and independent verification. Use when responding to an incident or continuing a repair session.
---

# Incident response

Accepted source and developer changes express intent. Operational memory is fallible advice. Recovery requires evidence from the intended application behavior. Discover capabilities and argument shapes through the local tool help.

1. Read `playbook_coverage`: matched finding fingerprints, bindings, mitigation capability and unexplained findings. Success and failure counts are information, never permission gates. A diagnosis-only match supplies evidence, not a repair. Check current status and any late findings before changing state.
2. Apply matched mitigations first after a quick live check of their preconditions and accepted-source values. Use supplied bindings and read each receipt. When repair scope is ambiguous, identify intended findings and use their fingerprints. Investigate contradictions rather than replaying stale assumptions. Keep track of findings still unexplained or unresolved.
3. Fix the rest yourself. Use recorded actions for live mutations and declare every changed resource; never hand-write receipts. Repair and commit source changes when needed. Restore namespace drift to accepted source unless live evidence establishes an intended difference; uncertainty alone does not block restoration. Explain benign drift with live evidence. Continue after a partial fix or failed mitigation. Keep operational memory read-only during response; report playbooks by logical ID.
4. Self-check with `sdo check`: fresh detectors then independent closure verification, also available separately as `sdo detect` and `sdo verify`. A successful command or plausible edit is not proof of recovery. Keep repairing while findings remain and budget permits; never claim done without this self-check. Cite live cause observations, explained_detectors and verification results. If findings cleared without a repair, return cancelled rather than inventing one.
5. Undo only on evidence that a change was wrong or harmful. Read the action report and current state first; do not undo useful repairs merely because another finding persists. Clean up temporary investigation resources and label helper Pods or Jobs `sdo.dev/responder-helper=true`, never application workloads.
6. On a re-prompt, work only the `still_broken` list. Read `already_done` and its receipts to preserve completed repairs, then self-check again. The controller independently checks the result after you return.

Write large tool output to a temporary file and grep or extract the relevant observations. Never infer missing evidence from a truncated prefix. Keep scratch files outside operational memory. Allow blocking operations to finish in one call instead of polling across model turns. Static files belong in static_context, not live cause evidence.
