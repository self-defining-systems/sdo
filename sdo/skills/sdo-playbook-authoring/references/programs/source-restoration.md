## Source restoration

Keep source-restoration mitigators for image, readiness and ConfigMap faults when diagnosis supports them.
Prefer a narrow, schema-checked SetField when diagnosis identifies the damaged field; named list selectors and
ConfigMap JSON/YAML entries preserve unrelated values. Use `program.FromSource(ref, path)` for an accepted-source field value,
and ReconcileFromSource only when restoring the whole source-owned object is justified. Structured effects use the ordinary namespace-scoped executor.
ReconcileFromSource and Rollback express desired state from accepted source or explicit history. Supply
accepted manifests in accepted.json for source-based replay. Replay observes the proposed action and subsequent diagnosis; unavailable effects remain unverified information.
Record typed SDK gaps for capabilities the repair needs but cannot express.
CreateFromSource restores only a named missing object present in accepted source. DeleteObject removes a
named object in the incident namespace; disclose deletion as irreversible, retain its receipt, and never
auto-recreate it. Live verification requires direct current UID/absence evidence and independent health checks.

RunScript bytes are checked against this program's original successful receipts in accepted memory at plan time.
Unlinked bytes are refused as `script_unprovenanced`; the automatic stage hands over without retrying.
The save-time static check gives early feedback but is not execution authority.

