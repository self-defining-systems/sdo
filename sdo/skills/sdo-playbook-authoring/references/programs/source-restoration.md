## Source restoration

Choose the playbook by failure mechanism, not merely the kind of object repaired.
A source-drift step diagnoses the bound object's difference from accepted source and restores that difference.
Admission mutation, genuine resource exhaustion and old-revision rollout blockage are independent mechanisms:
give them separate diagnoses and steps, even when they affect the same Deployment. Link the relevant playbooks
from findings and explain ordering in their READMEs. Keep coupled actions together only when one repair needs them.
Do not add unrelated observations as prerequisites to a working source-drift diagnosis; their unavailability
must not turn an otherwise decidable source comparison into Unknown. Refine the existing step for resource or
parameter variants; add a separate step when the cause or required evidence changes. Preserve earlier cases and
check both the repaired variant and prior variants when changing a shared step; replay remains informational.

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

