# Script receipt origins

A script's original successful incident receipt proves its bytes independently of replay.
Keep the playbook's `originating_incident` when updating it. For a later script revision,
or a legacy receipt without a captured script digest, link each script's own original receipt
in README front matter (shared helpers use `metadata.yaml`):

```yaml
script_receipts:
  <sha256 of exact script bytes>:
    incident_id: <original incident ID>
    receipt_id: <successful accepted action_id>
```

The check corroborates the reference against the independently accepted outcome. A recorded
script digest must match exactly. An unchanged script can retain its earlier accepted proof;
a new repair does not erase prior origins. Older `script_origins: {<digest>: <receipt-id>}`
with `originating_incident` remains readable for a single incident. Unknown or invented
receipts never authorize execution.

<!-- replay:on -->
When replay is enabled, also retain the case's `script_origins` linkage. Replay notes do
not replace the accepted receipt proof.
<!-- /replay:on -->
