# First-mutation evidence boundary

Controller-owned responder loops preserve `open@pre-apply.json` immediately
before the first live repair. `sdo act` requests the internal `pre-apply` tool
and waits for its acknowledgement before starting the local mutation or its
receipt. Typed `apply` and the trusted program fast path use the same helper.
The snapshot comes from the current provider for the current incident, not the
preserved detect snapshot. `open@detect` stays unchanged as historical evidence.

The first protected pre-apply file is retained across later commands and loop
restarts. Capture is best effort: an unavailable tool transport, response timeout,
invalid response, provider failure, or capture-write error must not block the
repair. `sdo act` emits a `pre-apply evidence gap` warning and still executes the
command, records its receipt, and returns its exit code. When the controller
observes capture failure it logs the cause and adds `pre_apply_capture_gap` to
`program_failure_signals`, retained in incident/revision-bound responder state.
Typed apply and the trusted fast path proceed through their normal mutation and
independent verification. The gap alone does not mean the repair failed. A later
request, including after state restore, cannot replace the missed first boundary
with a repaired snapshot. An existing successful capture is acknowledged without
replacement. This boundary performs no diagnosis, probes,
settle sleep, benchmark check, or external fault lookup. It adds one ordinary
snapshot and protected capture write per incident, plus the existing tool
request/response round trip for `sdo act`. Existing `responder_tool_timing`
`capture_ms` and `total_ms` include this work.

Captures do not invent primitive recordings. New pre-apply recordings begin
empty; only actual primitive reads may supply replay answers. A late ConfigMap
failure can be diagnosed from the Deployment reference, missing ConfigMap and
current Pod state in the snapshot. A pre-apply capture is later open evidence,
not proof of a fault type, listener behavior, or successful repair by itself.
