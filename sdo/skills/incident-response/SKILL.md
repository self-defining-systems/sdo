---
name: incident-response
description: Diagnose and repair application incidents.
---

# Incident response

- Run the failing operation and read the exact response. The opening alert may be a side effect.
- Look for changes on or upstream of the failing path near onset, or a latent trigger. Keep unexplained findings on that path open until checked. A passing operation proves liveness, not correct data for the failing operation.
- Verify suspected Secret rotation via authentication. Ownership timestamps alone are inconclusive.
- Name the top two explanations and test what best separates them.
- Predict what each change fixes and recheck once it takes effect. Unchanged failure counts against the hypothesis.
- **Restore lost data.** Restore from its authoritative copy first. Before recreating anything, check every app and deployment copy or backup: configuration, jobs, object stores, replicas. Never seed substitute data to pass health checks.
- Use the fastest supported operation with the same effect and side effects, including cache invalidation.
- Ensure the process uses the fix. Replace it if corrected configuration does not roll it.
- Verify the operation keeps passing. On pages return `completed` if verified, else `failed`. If fenced, finish your report. Later alerts can keep it open and escalated after you return.
- SDO ends detector-opened work on recovery. Wait with `sdo detect --wait SECONDS` (max 600), never `sleep`. It returns early only on state changes. Background results are interim. Keep working when it ends. The last result stands.
- Record `findings` even if giving up: observations and where, what you ruled out and why, and gaps. Omit probable causes. For `agent_changes`, give what, where and when (RFC 3339) for each change, empty if none.
