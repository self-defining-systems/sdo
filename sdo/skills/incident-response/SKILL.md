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
- **Hold a rival.** Test what splits the top two.
- **Correlate.** Journey path: `sdo map path OP`, `sdo timeline --component NODE`.
  Drift: `sdo changes`, `sdo source`. Store state: `sdo detect COMPONENT`.
  Stage writes, new findings, alert set: `sdo detect`.
- **Test each change.** Predict its fix; wait on its effect (`sdo detect --wait`), not a sleep. No effect counts against it; replace processes not yet using the fix.
- **Restore lost data.** Use its authoritative copy first. Before recreating, search all backups or copies: configuration, jobs, object stores, replicas. Never seed substitute data to pass health checks.
- **App-managed state** (schema, migrations, indexes, caches) behind its code or config: bring it current through the app's own maintenance path; hand edits are a last resort.
- **Late background work**: drain the queue (workers, poison jobs); never edit app code; note it if nothing drains.
- **Verify recovery.** Detector-opened: done is Recovered for all alerts, joined too, not one passing test. Paged: `completed` if verified, else `failed`.
  Fenced: return unless this incident's test fails; then keep working.
  Recovery covers all users. Probes sample user health. Never change state for probe/test identities. If the app serves others, verify with real traffic or a non-SDO request.
  Background results are interim. The last result stands.
- **Record.** `findings` (always required): evidence and where, what you ruled out and why, gaps.
  Omit probable causes. `agent_changes`: what, where, when (RFC 3339), or empty.
