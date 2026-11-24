---
name: incident-response
description: A general method for finding and removing what is hurting an application's users.
---

# Incident response

- **Look at what fails.** Run the failing operation and read its exact response. The opening alert can be a side effect.
- **Follow the change.** Something changed near onset on or upstream of the failing path (config, code, data, a
  dependency), or a latent condition was triggered. Check unexplained findings on that path.
  For suspected Secret rotation, verify the authentication failure. Ownership timestamps alone are inconclusive.
- **Hold a rival.** Name your lead explanation and the runner-up, and run the check whose outcome differs most.
- **Change as an experiment.** Predict which operations a change will fix, then check them. Prefer the
  fastest supported operation with the same effect and side effects, including cache invalidation.
- **Done** when the failing operation keeps passing. Verify the serving process uses the changed state.
  If corrected configuration does not roll that process, replace it. For paged incidents, return
  `completed` after verified recovery or `failed` if unverified.
  SDO ends detector-opened work on recovery. To wait, run
  `sdo detect --wait SECONDS` (up to 600), never `sleep`: it returns early only when the incident's state changes. A result submitted while a background command still runs is
  interim: you are woken when it finishes and must keep working; the last result stands.
- **Record.** `findings`: what you saw and where, what you ruled out and why (and what you did not check), never the
  probable cause; required, also when you give up. `agent_changes`: each change, what, where and when (RFC 3339);
  empty if none.
