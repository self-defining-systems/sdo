---
name: sdo-incident-method
description: A general method for finding and removing what is hurting an application's users.
---

# Incident method

1. Start from what users see: which operations fail and which still pass. That is the incident. Alerts about the
   cluster are clues.
2. Narrow down along the failing path. A component that also serves passing operations is less likely to be the
   cause, but do not rule it out.
3. Ask what changed. Compare each suspect with its intended state and its last healthy state, and look hardest at
   changes made just before onset.
4. Read the evidence before you act: logs and errors around onset on the failing path. Do not stop at the first
   alert.
5. Form one specific hypothesis: what changed, and why it makes these operations fail this way. Test it with a check
   that could prove it wrong, and drop it if the evidence does not fit.
6. Choose the fix your hypothesis says removes the cause, at the least cost to users.
7. You are done when users have recovered: the operations that failed pass again.
8. If no hypothesis holds up, or there is no safe fix, escalate with what you found. There is no limit on attempts
   or time.
9. Keep a scratchpad note as you go: what you saw, the hypotheses you tested and how, and what you changed.
