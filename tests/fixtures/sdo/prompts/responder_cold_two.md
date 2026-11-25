# Incident response

- **Look at what fails.** Run the failing operation and read its exact response. The alert can be a side effect.
- **Follow the change.** Look for changes on or upstream of the failing path near onset, or a latent trigger.
  Direct writes can leave `updated_at` old: still suspect the row.
  Check all unexplained findings there.
  Passing proves liveness only.
  See stage writes in `sdo detect`. Own repairs after handoff. Test auth on Secret drift.
- **Hold a rival.** Check what best distinguishes the top two explanations.
- **Test each change.** Predict its fix, recheck after it takes effect. Unchanged failure counts against the hypothesis.
- **Restore lost data.** Restore from its authoritative copy first. Before recreating, search all app and deployment backups or copies: configuration, jobs, object stores, replicas. Never seed substitute data to pass health checks.
- Prefer the fastest supported operation with the same effects, including cache invalidation.
- Ensure the process uses the fix. Replace it if not rolled.
- **Verify recovery.** The operation must keep passing. For paged incidents, return `completed` if verified, else `failed`.
  If fenced, finish your report. Later alerts can leave it open and escalated.
  Recovery covers all users. Probes sample user health. Never change state for probe/test identities. If the app serves others, verify with real traffic or a non-SDO request.
  Background results are interim. Keep working when it ends. The last result stands.
- **Record.** `findings` (required even if you give up): what you saw and where, what you ruled out and why, and gaps.
  Omit probable causes. `agent_changes`: what, where, when (RFC 3339), empty if none.

Incident toy-incident in namespace toy: investigate and fix it. You have kubectl, the shell and the `sdo` command (skills at /opt/sdo/skills). On detector alerts, SDO ends work on recovery; return only to give up. Never print a credential value.
Opened on Service/component-0 unavailable at 22:13:20 UTC.
Candidates: `sdo detect`; every change: `sdo changes`; signals over time: `sdo timeline`.
Answer in the `incident_result` field and leave `reflection` null.
Additional environment-specific instructions:
Task telemetry (mode open): no source is bound; omit telemetry detectors. Bindings are read-only.
