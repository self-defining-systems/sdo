---
name: sdo-incident-response
description: Resolve incidents using learned playbook coverage, live repair evidence, detector self-checks, and independent verification. Use when responding to an incident or continuing a repair session.
---

# Incident response

Accepted source and developer changes express intent. Memory is fallible advice; intended application behavior is the recovery test. Discover capabilities and argument shapes through the local tool help.

1. Read `playbook_coverage`: which finding fingerprints each match explains, its bindings and mitigation capability, and which findings remain unexplained. Success and failure counts are information, never permission gates. A diagnosis-only match supplies evidence, not a repair. Check current status and any late findings before changing state.
2. Apply matched mitigations first after a quick live check of their preconditions and accepted-source values. Use each applicable mitigation with its supplied bindings; read its receipt before moving on. Investigate contradictions rather than replaying stale assumptions. Keep track of findings still unexplained or unresolved.
3. Fix the rest yourself. Use recorded actions for live mutations and declare every changed resource; let the recorder produce receipts. Repair and commit source changes when needed. Continue after a partial fix or failed mitigation. Keep operational memory read-only during response; report playbooks by logical ID.
4. Self-check the findings you worked on with fresh `sdo detect` results, then run `sdo verify` for independent closure checks. A successful command or plausible edit is not proof of recovery. Keep repairing while findings remain and budget permits; never claim done without this self-check. Cite live observations for causes and verification results for recovery. If findings cleared without a repair, report that honestly rather than inventing one.
5. Undo only on evidence that a change was wrong or harmful. Read the action report and current state first; do not undo useful repairs merely because another finding persists. Clean up temporary investigation resources and label helper Pods or Jobs `sdo.dev/responder-helper=true`, never application workloads.
6. On a re-prompt, work only the `still_broken` list. Read `already_done` and its receipts to preserve completed repairs, then self-check again. The controller independently checks the result after you return.

Write large tool output to a temporary file and grep or extract the relevant observations. Never infer missing evidence from a truncated prefix. Keep scratch files outside operational memory.
