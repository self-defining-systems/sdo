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

## Cold developer source revisions

A harness `developer_source_receipt.json` records a pre-controller source patch:
`parent`, `commit`, `patch_sha256`, and, for a newly created commit, `memory_tree`.
Resolve both Git commits and require the patch commit to preserve the `.sdo`
tree. The idempotent replay receipt can omit `memory_tree`; derive it from the
recorded commit. This is setup provenance, not a broker acceptance, live
operator update, incident outcome, or trust credit. A later stage may inherit
accepted learning as well as developer commits; compare actual artifact hashes
before attributing a verdict to the original memory.

Scripted artifact probes can compile unchanged learned programs against a
current SDK and feed synthetic or real cluster observations. Require the
artifact hashes, source revision, observation/probe provenance and binding.
Distinguish hermetic fakes from live reads. Such captures demonstrate artifact
behavior, not model recovery or full controller execution. A proposed action
is not an applied action. Require strict receipts and proxy history for writes,
verification and accepted learning; retain missing evidence as unknown. A
healthy-source negative with stale health alarms is a false alarm even if the
normal fault gate blocks. Do not bypass that gate or invent an injected fault.


## Scoped observation captures

`scoped_observation` version 1 contains accepted `generation`, versioned `scope`, per-target `statuses` and credential-free foreign objects. Targets use API version, kind, namespace and (for cluster webhooks) name. Generation binds accepted commit and policy identities; do not compare object inventories across generations as drift.

Only `observed` means a complete read, including a successful empty list or named webhook NotFound. `forbidden`, `unsupported` and `unavailable` are evidence gaps, never absence or health. Captures retain those statuses for offline accessor errors. A denied detector dependency cannot clear a finding; incomplete coverage suspends healthy baseline promotion. Regrant requires a fresh read and retains comparison with the prior observed state.

Foreign Deployment captures exclude all environment values/references, command/args, volumes, probes, annotations and free-text status. Webhook URLs retain origin only; CA bytes are digest markers. Service/Endpoints projections exclude arbitrary metadata and endpoint target references. These are safe views, not complete Kubernetes backups. App-local ConfigMap capture-only redaction is unchanged. Scoped polling uses no foreign Secret reads and no webhook-wide list.

Incident requests and closures persist `observation_generation`. A closure may
reach the broker only after a fresh snapshot confirms its verification generation;
a restored or adopted closure from an obsolete scope fails visibly and cannot
publish retained success. A scope change during an open incident supersedes its
evidence and produces a failed closure. `open@detect` remains historical evidence;
diagnosis rejects reuse when its generation differs from the current snapshot.
