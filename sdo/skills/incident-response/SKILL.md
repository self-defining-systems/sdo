---
name: incident-response
description: A general method for finding and removing what is hurting an application's users.
---

# Incident response

1. Start from what failed: the user operations that fail (and which still pass), or the part of the system that
   stopped healing itself. That is the incident. Every other alert is a clue.
2. Narrow down along the failing path. A component that also serves passing operations is less likely to be the
   cause, but do not rule it out.
3. Ask what changed. Compare each suspect with its intended state and its last healthy state, and look hardest at
   changes made just before onset.
4. Read the evidence before you act: logs and errors around onset on the failing path.
5. Form one specific hypothesis: what changed, and why it makes these operations fail this way. Test it with a check
   that could prove it wrong, and drop it if the evidence does not fit.
6. Decide what to change from your hypothesis.
7. You are done when what failed passes again and keeps passing.
8. If no hypothesis holds up, or there is no safe fix, stop and escalate with what you found.
9. Keep a short scratchpad note as you go: what you saw, what you tested and how, and what you changed.
