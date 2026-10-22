# First-mutation evidence boundary

Controller-owned responder loops preserve `open@pre-apply.json` immediately
before the first live repair. `sdo act` requests the internal `pre-apply` tool
and waits for its acknowledgement before starting the local mutation or its
receipt. Typed `apply` and the trusted program fast path use the same helper.
The snapshot comes from the current provider for the current incident, not the
preserved detect snapshot. `open@detect` stays unchanged as historical evidence.

The first protected pre-apply file is retained across later commands and loop
restarts. A capture error prevents the pending mutation; a repeated request
acknowledges the existing file. This boundary performs no diagnosis, probes,
settle sleep, benchmark check, or external fault lookup. It adds one ordinary
snapshot and protected capture write per incident, plus the existing tool
request/response round trip for `sdo act`. Existing `responder_tool_timing`
`capture_ms` and `total_ms` include this work.

Captures do not invent primitive recordings. New pre-apply recordings begin
empty; only actual primitive reads may supply replay answers. A late ConfigMap
failure can be diagnosed from the Deployment reference, missing ConfigMap and
current Pod state in the snapshot. A pre-apply capture is later open evidence,
not proof of a fault type, listener behavior, or successful repair by itself.
