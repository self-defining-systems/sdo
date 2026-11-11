---
name: sdo-incident-navigation
description: Investigate an incident before repairing it. User impact, scope, evidence, ranked hypotheses, least disruptive fix, user-facing verification, early escalation, a trail for reflection.
---

# Navigating an incident

Use this method for whatever stage receipts, coverage and learned playbooks leave unexplained. Investigate before you mutate; each step should shrink the set of possible causes.

1. **Start from user impact.** Name which user operations fail, since when and how (errors, latency, wrong results), from SDO's traffic probes and findings. Pod health is not service health, and a restart is not recovery.
2. **Scope the failing path.** Follow the application map's edges from the failing operation to what it depends on. Look for what changed: drift from accepted source (`sdo status`, `sdo source`), recent rollouts, events. The component that changed or behaves differently is the first suspect, not the one that complains loudest.
3. **Read the evidence before acting.** Read the caller's and callee's logs around the first failure, not only the latest lines. Use the application's own health, self-check or admin commands when it ships them, and check the state of anything the failing path depends on. Read accepted source before raw commands.
4. **Rank hypotheses and discriminate.** Write down two or three candidate causes. Run the cheapest check whose result differs between them. Separate cause from symptom: a cause explains every observation and its timing; a symptom goes away when its cause is removed. Independent causes can coexist; do not stop at the first.
5. **Fix the cause with the least disruption.** Prefer restoring accepted or declared state, reverting the change, or a documented recovery procedure over restarts and deletions. Never reduce serving capacity or disturb data-holding workloads just to clear a signal. A change that only silences a detector is not a fix.
6. **Verify where users are.** Confirm the failing operations succeed again through `sdo verify` and its traffic checks, not only the detector that opened the incident. If the failure returns, the hypothesis was wrong: go back to step 4 with the new evidence.
7. **Escalate early.** When the evidence places the cause outside what SDO may change, stop repairing and report it as out of reach with that evidence.
8. **Leave a trail.** In your result, name the checks that discriminated the cause and the fix steps in order, with the resources each touched. Reflection compiles the checks into a playbook's Diagnose and the steps into its program, so a repeat skips the investigation.
