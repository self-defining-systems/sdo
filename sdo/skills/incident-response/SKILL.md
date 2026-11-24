---
name: incident-response
description: Incident repair.
---

# Incident response

- **Look at what fails.** Run the failing operation and read its exact response. The opening alert can be a side effect.
- **Follow the change.** Look for changes on or upstream of the failing path near onset, or a latent trigger.
  Check all unexplained findings there.
  Passing shows liveness, not correct data for the failing operation.
  See stage writes in `sdo detect`. You own repairs after handoff. Test auth on Secret drift.
- **Hold a rival.** Name the top two explanations and run the check that best distinguishes them.
- **Test each change.** Predict what it fixes, then recheck after it takes effect. Unchanged failure counts against the hypothesis.
- **Restore lost data.** Restore from its authoritative copy first. Before recreating, search all app and deployment backups or copies: configuration, jobs, object stores, replicas. Never seed substitute data to pass health checks.
- Prefer the fastest supported operation with the same effect and side effects, including cache invalidation.
- Ensure the process uses the fix. Replace it if the fix does not roll it.
- **Verify recovery.** The operation must keep passing. For paged incidents, return `completed` if verified, else `failed`.
  If fenced, finish your report. Later alerts can leave it open and escalated.
  SDO ends detector-opened work on recovery. Use `sdo detect --wait SECONDS` (max 600), never `sleep`. Returns early on state changes. Background results are interim. Keep working when it ends. The last result stands.
- **Record.** `findings` (required even if you give up): what you saw and where, what you ruled out and why, and gaps.
  Omit probable causes. `agent_changes`: what, where, when (RFC 3339), empty if none.
