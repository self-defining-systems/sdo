These fixtures exercise the learned-plan boundary without Kubernetes or model
calls. Run from `sdo/operator/runtime`:

```bash
go test . -run TestPlanExecutorSourceAuthoritySidecarEvidence -v -count=1
```

`sidecar_reduced.json` is reduced from the SP1 hermetic probe's stage 1 input
and output (`configs/SP1/hermetic.py` and `probe_main.go`). It retains accepted
Service and Deployment objects, live Service drift, the learned 9100 action,
and its drift event.

`sp1_stage1_replay.json` replays incident
`hotel-reservation-1792721572314399383`. Accepted objects are the archived
`source` tool responses; the action and drift event come from the committed
outcome summary. The live input is the proxy's last observed Service at event
6. Its continuity is Unknown: this replay proves how the guard handles those
archived observations, not continuity up to the write or changed model behavior.
The archived actual write is established separately by the outcome's typed
application receipt, not inferred from that proxy observation.

Each fixture records the original evidence paths under the external mixed10
experiment directory and their SHA-256 hashes. No fixture is a recovery verdict.
The tests refuse the original 9999-to-9100 action before any write and permit
an 8083 restoration, preserving the metrics sidecar and undeclared application
port. The guard ignores the historical/displayed `source` string in drift events
and independently obtains accepted values through PlanSource.
