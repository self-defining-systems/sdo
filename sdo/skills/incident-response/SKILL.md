---
name: incident-response
description: A general method for finding and removing what is hurting an application's users.
---

# Incident response

- **Look at what fails.** Run the failing operation and read the exact response. The opening alert can be a side effect.
- **Follow the change.** Something changed near onset on or upstream of the failing path (config, code, data, a
  dependency), or a latent condition was triggered. An unexplained finding on that path stays open until checked.
  A passing operation shows a node is alive, not that its data for the failing one is right.
  For suspected Secret rotation, verify the authentication failure. Ownership timestamps alone are inconclusive.
- **Hold a rival.** Name your lead explanation and the runner-up, and run the check whose outcome differs most.
- **Change as an experiment.** Say which operations a change should fix and check those after its delay. A change that left the failure as is counts against its hypothesis.
- **Done** when the failing operation keeps passing. For a page-opened incident, verify recovery and return `completed`, or `failed` if you cannot.
  Recovery can be provisional. If SDO fenced your writes, finish the report even if an alert joins.
  To wait, run
  `sdo detect --wait SECONDS` (up to 600), never `sleep`: it returns early only when the incident's state changes. A result submitted while a background command still runs is
  interim: you are woken when it finishes and must keep working; the last result stands.
- **Record.** `findings`: what you saw and where, what you ruled out and why (and what you did not check), never the
  probable cause; required, also when you give up. `agent_changes`: each change, what, where and when (RFC 3339);
  empty if none.
