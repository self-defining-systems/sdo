---
name: incident-response
description: Incident repair.
---

# Incident response

- **Look at what fails.** Run the failing operation; read its exact response. The alert can be a side effect.
- **Follow the change.** Changes on or upstream of the failing path near onset, or a latent trigger.
  Direct writes can leave `updated_at` old: suspect the row.
  Faults can coexist: work every failing signal in parallel; explain every finding.
  Passing proves liveness only.
  Own repairs after handoff. Test auth on Secret drift.
- **Hold a rival.** Test what splits the top two explanations.
- **Correlate.** Failing journey to path: `sdo map path OP`, `sdo timeline --component`.
  Drift: `sdo changes`, `sdo source`. Store vs healthy: `sdo detect COMPONENT`.
  Stage writes, new findings, alert set: `sdo detect`.
- **Test each change.** Predict its fix; wait on its effect (`sdo detect --wait`), not a sleep. No effect refutes it.
- **Restore lost data.** Restore from its authoritative copy first. Before recreating, search all backups or copies: configuration, jobs, object stores, replicas. Never seed substitute data to pass health checks.
- **Late background work**: drain the queue (workers, poison jobs); never edit app code; escalate if nothing drains.
- **Verify recovery.** Done is SDO's Recovered for every alert, joined too; one test passing is not. Paged: `completed` if verified, else `failed`.
  If fenced, return unless this incident's test fails; then keep working.
  Recovery covers all users. Probes sample user health. Never change state for probe/test identities. If the app serves others, verify with real traffic or a non-SDO request.
  Background results are interim. The last result stands.
- **Record.** `findings` (required even if you give up): evidence and where, what you ruled out and why, and gaps.
  Omit probable causes. `agent_changes`: what, where, when (RFC 3339), empty if none.
