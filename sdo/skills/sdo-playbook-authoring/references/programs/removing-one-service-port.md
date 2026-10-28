## Removing one Service port

Use `RemoveServicePort{Ref: service, Name: observedName, Reason: reason}` to remove exactly one
unwanted port without restoring a stale source Service. Alternatively select `Port` and optional
`Protocol` (TCP, UDP or SCTP); name and number are mutually exclusive. Missing and ambiguous entries
refuse before writing. The last port is protected unless `AllowEmpty` is explicit; API validation still
applies. The executor uses UID/version-conditional patches, proves the resulting list by direct read,
and retains the preimage for conflict-checked compensation. Keep a hand-written open/fixed replay case
showing the removed listener and preservation of other ports and Service fields.

