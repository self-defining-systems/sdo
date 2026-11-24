---
name: incident-response
description: A general method for finding and removing what is hurting an application's users.
---

# Incident response

- **Look at what fails.** Run the failing operation and read its exact response. The opening alert can be a side effect.
- **Follow the change.** Look for changes on or upstream of the failing path near onset, or a latent trigger.
  Keep unexplained findings on that path open until checked.
  A passing operation shows a node is alive, not that its data for the failing one is right.
  Verify suspected Secret rotation by checking authentication. Ownership timestamps alone are inconclusive.
- **Hold a rival.** Name the top two explanations and run the check that best distinguishes them.
- **Test each change.** Predict what it fixes, then recheck after it takes effect. Unchanged failure counts against the hypothesis.
- Prefer the fastest supported operation with the same effect and side effects, including cache invalidation.
- Done when the serving process uses the changed state. Replace it if corrected configuration does not roll it.
- **Verify recovery.** The operation must keep passing. For paged incidents, return `completed` if verified, else `failed`.
  If fenced, finish your report. A later alert can keep the incident open and escalated after you return.
  SDO ends detector-opened work on recovery. Wait with `sdo detect --wait SECONDS` (max 600), never `sleep`.
  It returns early only on state changes. Background results are interim. Resume when the command ends. The last result stands.
- **Record.** `findings` (required even if you give up): what you saw and where, what you ruled out and why, and gaps.
  Keep probable causes out. `agent_changes`: what, where and when (RFC 3339) for each change, empty if none.
