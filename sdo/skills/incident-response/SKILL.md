---
name: incident-response
description: Incident repair.
---

# Incident response

- **Look at what fails.** Run the failing operation; read its exact response. The alert can be a side effect.
- **Follow the change.** Look for changes on or upstream of the failing path near onset, or a latent trigger.
  Direct writes can leave `updated_at` old: still suspect the row.
  Faults can coexist: work every failing signal in parallel; explain every finding.
  Passing proves liveness only.
  See stage writes in `sdo detect`. Own repairs after handoff. Test auth on Secret drift.
- **Hold a rival.** Check what best splits the top two explanations.
- **Test each change.** Predict its fix; `sdo detect --wait` returns when its result is in, never a fixed sleep. No change counts against the hypothesis.
- **Restore lost data.** Restore from its authoritative copy first. Before recreating, search all app and deployment backups or copies: configuration, jobs, object stores, replicas. Never seed substitute data to pass health checks.
- Prefer the fastest supported equivalent, including for cache invalidation.
- **Late background work**: drain the queue (workers, bulk work, poison jobs); never edit app code; escalate if nothing drains.
- Replace processes not yet using the fix.
- **Verify recovery.** The operation must keep passing. For paged incidents, return `completed` if verified, else `failed`.
  If fenced, return unless a test of this incident fails; then keep working.
  Recovery covers all users. Probes sample user health. Never change state for probe/test identities. If the app serves others, verify with real traffic or a non-SDO request.
  Background results are interim. Keep working when it ends. The last result stands.
- **Record.** `findings` (required even if you give up): evidence and where, what you ruled out and why, and gaps.
  Omit probable causes. `agent_changes`: what, where, when (RFC 3339), empty if none.
